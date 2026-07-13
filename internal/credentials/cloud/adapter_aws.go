package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	awsclient "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

const (
	accessTokenField          = "access_token"
	kubeconfigKind            = "Config"
	principalFieldID          = "principal"
	principalFieldDescription = "The IAM Principal identifier"
)

type AwsAdapter struct {
	AwsTemporaryCredsProvider *AwsTemporaryCredsProvider
	AwsCredsClient            awsclient.CredentialsClientInterface
	Logger                    *zap.Logger
}

var _ TypedAdapter = (*AwsAdapter)(nil)

func (a *AwsAdapter) Exchange(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration, args ExchangeArgs) (map[string]interface{}, error) {
	config, err := runnerConfig.AsK8sEksRunnerConfiguration()
	if err != nil {
		return nil, fmt.Errorf("invalid EKS Cluster runner configuration: %w", err)
	}

	if t, err := a.AwsTemporaryCredsProvider.ExchangeOauthToken(ctx, orgId, runnerId, config.Cluster.Region, config.Cluster.Auth); err != nil {
		return nil, err
	} else {
		return map[string]interface{}{
			accessTokenField: t.AccessToken,
			"expiry":         t.Expiry.Format(time.RFC3339Nano),
		}, nil
	}
}

func (a *AwsAdapter) Check(ctx context.Context, outputCredential map[string]interface{}) ([]CheckCredentialsSuccess, []Warning, error) {
	if at, ok := outputCredential[accessTokenField].(string); !ok {
		return nil, nil, fmt.Errorf("access_token missing from credential")
	} else if ti, err := a.AwsCredsClient.GetAccessTokenInfo(ctx, at); err != nil {
		return nil, nil, fmt.Errorf("failed to get token info: %w", err)
	} else {
		fields := make([]CheckCredentialsSuccess, 0)
		if ti.Principal != "" {
			fields = append(fields, CheckCredentialsSuccess{
				Id:          principalFieldID,
				Description: principalFieldDescription,
				Value:       ti.Principal,
			})
		}
		return fields, []Warning{}, nil
	}
}

func (a *AwsAdapter) GetKubeconfig(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration) (*clientcmdapi.Config, error) {
	config, err := runnerConfig.AsK8sEksRunnerConfiguration()
	if err != nil {
		return nil, fmt.Errorf("invalid EKS Cluster runner configuration: %w", err)
	}

	stsRegion := config.Cluster.Region
	if config.Cluster.Auth.StsRegion != nil {
		stsRegion = *config.Cluster.Auth.StsRegion
	}
	clusterName := config.Cluster.Name
	clusterRegion := config.Cluster.Region

	if t, err := a.AwsTemporaryCredsProvider.ExchangeOauthToken(ctx, orgId, runnerId, config.Cluster.Region, config.Cluster.Auth); err != nil {
		return nil, err
	} else {
		awsCredentials, err := extractAwsCredentials(t)
		if err != nil {
			return nil, fmt.Errorf("failed to extract AWS credentials: %w", err)
		}

		clusterInfo, err := a.AwsCredsClient.GetClusterInfo(ctx, clusterName, clusterRegion, awsCredentials)
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster info: %w", err)
		}

		token, err := a.AwsCredsClient.GenerateEksToken(ctx, clusterName, stsRegion, awsCredentials)
		if err != nil {
			return nil, fmt.Errorf("failed to generate EKS token: %w", err)
		}

		config := &clientcmdapi.Config{
			APIVersion: "v1",
			Kind:       kubeconfigKind,
			Clusters: map[string]*clientcmdapi.Cluster{
				clusterName: {
					Server:                   clusterInfo.Endpoint,
					CertificateAuthorityData: clusterInfo.CertificateAuthorityData,
				},
			},
			Contexts: map[string]*clientcmdapi.Context{
				clusterName: {
					Cluster:  clusterName,
					AuthInfo: clusterName,
				},
			},
			CurrentContext: clusterName,
			AuthInfos: map[string]*clientcmdapi.AuthInfo{
				clusterName: {
					Token: token,
				},
			},
		}

		return config, nil
	}
}

func extractAwsCredentials(t *oauth2.Token) (awsclient.AWSCredentials, error) {
	var awsCredentials awsclient.AWSCredentials
	if err := json.Unmarshal([]byte(t.AccessToken), &awsCredentials); err != nil {
		return awsclient.AWSCredentials{}, fmt.Errorf("failed to parse AWS credentials from token: %w", err)
	}

	if awsCredentials.AccessKeyID == "" {
		return awsclient.AWSCredentials{}, fmt.Errorf("access_key_id not found in AWS credentials")
	}

	if awsCredentials.SecretAccessKey == "" {
		return awsclient.AWSCredentials{}, fmt.Errorf("secret_access_key not found in AWS credentials")
	}

	if awsCredentials.SessionToken == "" {
		return awsclient.AWSCredentials{}, fmt.Errorf("session_token not found in AWS credentials")
	}
	return awsCredentials, nil
}

func parseSessionName(orgId string, runnerId string, defaultSessionName *string) string {
	sessionName := ref.DerefOr(defaultSessionName, fmt.Sprintf("%s-%s", orgId, runnerId))
	const maxSessionNameLength = 64
	if len(sessionName) > maxSessionNameLength {
		sessionName = sessionName[:maxSessionNameLength]
	}
	return sessionName
}

func parseSubject(orgId string, runnerId string) string {
	subject := fmt.Sprintf("%s+%s", orgId, runnerId)
	const maxSubjectNameLength = 255
	if len(subject) > maxSubjectNameLength {
		subject = subject[:maxSubjectNameLength]
	}
	return subject
}

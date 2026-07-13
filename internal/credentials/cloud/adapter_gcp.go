package cloud

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/gcp"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

// ClusterManagerClient Interface for mocking out of the GCP API
type ClusterManagerClient interface {
	GetCluster(context.Context, *containerpb.GetClusterRequest, ...gax.CallOption) (*containerpb.Cluster, error)
	Close() error
}
type GcpAdapter struct {
	GcpCredsClient gcp.CredentialsClientInterface
	OidcProvider   oidc.Provider
	Logger         *zap.Logger
	// To allow for injection of test version of GCP API
	NewClusterManagerClient func(context.Context, ...option.ClientOption) (ClusterManagerClient, error)
}

func (a *GcpAdapter) Exchange(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration, args ExchangeArgs) (map[string]interface{}, error) {
	if a.OidcProvider == nil {
		return nil, errors.New("oidc provider not configured")
	}
	config, err := runnerConfig.AsK8sGkeRunnerConfiguration()
	if err != nil {
		return nil, errors.Wrap(err, "invalid GKE Cluster runner configuration")
	}
	audience := config.Cluster.Auth.GcpAudience
	serviceAccount := config.Cluster.Auth.GcpServiceAccount
	subject := fmt.Sprintf("%s+%s", orgId, runnerId)

	if idToken, err := a.OidcProvider.CreateToken(ctx, subject, audience); err != nil {
		return nil, errors.Wrap(err, "failed to make create id token request")
	} else if ts, err := a.GcpCredsClient.GetExternalAccountTokenSource(ctx, audience, serviceAccount, idToken, args.AdditionalOAuthScopes); err != nil {
		return nil, errors.Wrap(err, "failed to get token source")
	} else if token, err := ts.Token(); err != nil {
		if parsedErr := parseGoogleOAuthErrorAsUserError(a.Logger, err); parsedErr != nil {
			return nil, parsedErr
		}
		return nil, errors.Wrap(err, "failed to get token")
	} else {
		return map[string]interface{}{
			accessTokenField: token.AccessToken,
			"expiry":         token.Expiry.Format(time.RFC3339Nano),
		}, nil
	}
}

func (a *GcpAdapter) Check(ctx context.Context, outputCredential map[string]interface{}) ([]CheckCredentialsSuccess, []Warning, error) {
	if at, ok := outputCredential[accessTokenField].(string); !ok {
		return nil, nil, errors.Errorf("access_token missing from credential")
	} else if ti, err := a.GcpCredsClient.GetAccessTokenInfo(ctx, at); err != nil {
		return nil, nil, errors.Errorf("failed to get token info")
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

func (a *GcpAdapter) GetKubeconfig(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration) (*clientcmdapi.Config, error) {
	if a.NewClusterManagerClient == nil {
		a.NewClusterManagerClient = func(ctx context.Context, opts ...option.ClientOption) (ClusterManagerClient, error) {
			return container.NewClusterManagerClient(ctx, opts...)
		}
	}

	config, err := runnerConfig.AsK8sGkeRunnerConfiguration()
	if err != nil {
		return nil, usererrors.NewUserError(fmt.Sprintf("invalid GKE Cluster runner configuration for runner %q: %s", runnerId, err.Error()))
	}

	data, err := a.Exchange(ctx, orgId, runnerId, runnerConfig, ExchangeArgs{})
	if err != nil {
		return nil, err
	}
	accessToken, _ := data[accessTokenField].(string)
	client, err := a.NewClusterManagerClient(ctx, option.WithCredentials(&google.Credentials{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken}),
	}))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cluster manager client")
	}
	defer func() {
		_ = client.Close()
	}()

	clusterDescription, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s", config.Cluster.ProjectId, config.Cluster.Location, config.Cluster.Name),
	})
	if err != nil {
		return nil, usererrors.NewUserError(fmt.Sprintf("failed to get cluster description for runner %q: %s", runnerId, err.Error()))
	}

	endpoint, err := getClusterEndpoint(clusterDescription, config.Cluster)
	if err != nil {
		return nil, usererrors.NewUserError(fmt.Sprintf("failed to retrieve GKE cluster endpoint for runner %q: %s", runnerId, err.Error()))
	}
	cert, err := base64.StdEncoding.DecodeString(clusterDescription.MasterAuth.ClusterCaCertificate)
	if err != nil {
		return nil, usererrors.NewUserError(fmt.Sprintf("failed to decode ca certificate for GKE cluster %q: %s", clusterDescription.Name, err.Error()))
	}
	return &clientcmdapi.Config{
		APIVersion: "v1",
		Kind:       kubeconfigKind,
		Clusters: map[string]*clientcmdapi.Cluster{
			clusterDescription.Id: {
				CertificateAuthorityData: cert,
				Server:                   endpoint,
				ProxyURL:                 ref.DerefOr(config.Cluster.ProxyUrl, ""),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			clusterDescription.Id: {
				Token: accessToken,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			clusterDescription.Id: {
				Cluster:  clusterDescription.Id,
				AuthInfo: clusterDescription.Id,
			},
		},
		CurrentContext: clusterDescription.Id,
	}, nil
}

var _ TypedAdapter = (*GcpAdapter)(nil)

type googleIamErrorBody struct {
	Error googleIamErrorBodyInner `json:"error"`
}

type googleIamErrorBodyInner struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type genericOauthBody struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func parseGoogleOAuthErrorAsUserError(logger *zap.Logger, err error) error {
	if v := new(oauth2.RetrieveError); errors.As(err, &v) {
		// some edge cases in the oauth library return an unpopulated retrieve error
		if v.ErrorCode == "" && v.Body != nil {
			var oauthErrorBody genericOauthBody
			if parseErr := json.Unmarshal(v.Body, &oauthErrorBody); parseErr == nil {
				v.ErrorCode = oauthErrorBody.Error
				v.ErrorDescription = oauthErrorBody.ErrorDescription
			}
		}
		if v.ErrorCode == "" {
			v.ErrorCode = v.Response.Status
			v.ErrorDescription = v.Response.Request.URL.String()
		} else if v.ErrorDescription == "" {
			v.ErrorDescription = "unknown"
		}
		return usererrors.NewUserError(fmt.Sprintf("failed to fetch token: %s: %s", v.ErrorCode, v.ErrorDescription))
	} else if p := strings.Split(err.Error(), "oauth2/google: status code 4"); len(p) > 1 && len(p[1]) > 2 {
		var oauthErrorBody genericOauthBody
		var iamErrorBody googleIamErrorBody
		if parseErr := json.Unmarshal([]byte(strings.TrimSpace(p[1][3:])), &oauthErrorBody); parseErr == nil {
			return usererrors.NewUserError(fmt.Sprintf("failed to exchange token: %s: %s", oauthErrorBody.Error, oauthErrorBody.ErrorDescription))
		} else if parseErr := json.Unmarshal([]byte(strings.TrimSpace(p[1][3:])), &iamErrorBody); parseErr == nil {
			return usererrors.NewUserError(fmt.Sprintf("failed to exchange token: %s: %s", iamErrorBody.Error.Status, iamErrorBody.Error.Message))
		} else {
			logger.Warn("unparsable 4xx error", zap.String("body", p[1][3:]), zap.String("details", parseErr.Error()))
		}
	} else if strings.HasPrefix(err.Error(), "oauth2: ") {
		logger.Warn("unparsable oauth error", zap.String("details", err.Error()))
		return usererrors.NewUserError("failed to fetch token")
	} else if strings.HasPrefix(err.Error(), "private key should be") {
		return usererrors.NewUserError("invalid private key")
	}
	return nil
}

func getClusterEndpoint(clusterDescription *containerpb.Cluster, config platformorchestratorcp.K8sRunnerGkeCluster) (string, error) {
	endpoint := clusterDescription.Endpoint
	// If the cluster has a private endpoint and the definition lists that it may use it, we can use this. This avoids
	// issues with NAT, authorized networks, and the public endpoint.
	if config.InternalIp != nil && *config.InternalIp {
		if config.ProxyUrl == nil {
			return "", errors.Errorf("private endpoint is requested, but proxy URL is not defined")
		} else if clusterDescription.ControlPlaneEndpointsConfig != nil && clusterDescription.ControlPlaneEndpointsConfig.IpEndpointsConfig != nil && clusterDescription.ControlPlaneEndpointsConfig.IpEndpointsConfig.PrivateEndpoint != "" {
			endpoint = clusterDescription.ControlPlaneEndpointsConfig.IpEndpointsConfig.PrivateEndpoint
		} else {
			return "", errors.Errorf("no private endpoint configured for GKE cluster %s", clusterDescription.Name)
		}
	}
	return fmt.Sprintf("https://%v", endpoint), nil
}

package cloud

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
	"golang.org/x/oauth2"

	awsclient "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	aws_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws/mocks"
	oidc_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc/mocks"
)

const (
	testStsRegion   = "us-west-2"
	secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" // //nolint:gosec // fake ARN values used only in test fixtures
	sessionToken    = "FwoGZXIvYXdzEBYaDEXAMPLE"                 // //nolint:gosec // fake ARN values used only in test fixtures
)

func TestAwsAdapter(t *testing.T) {
	t.Run("Exchange - nominal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		//nolint:gosec // G101: These are test credentials, not real ones
		expectedAccessToken := `{"access_key_id":"AKIAIOSFODNN7EXAMPLE","secret_access_key":"` + secretAccessKey + `","session_token":sessionToken,"region":"` + testStsRegion + `","expiration":"2023-01-01T00:00:00Z"}`
		expectedExpiry := time.Now().Add(time.Hour)
		stsRegion := testStsRegion

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: expectedAccessToken,
				Expiry:      expectedExpiry,
			}), nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		cred, err := adapter.Exchange(context.Background(), "some-org", "some-runner", eksCfg, ExchangeArgs{})
		require.NoError(t, err)
		assert.Equal(t, expectedAccessToken, cred["access_token"])
		assert.Equal(t, expectedExpiry.Format(time.RFC3339Nano), cred["expiry"])
	})

	t.Run("Exchange - token retrieval error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(&errorTokenSource{Error: errors.New("token retrieval failed")}, nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		_, err := adapter.Exchange(context.Background(), "some-org", "some-runner", eksCfg, ExchangeArgs{})
		require.EqualError(t, err, "failed to get token: token retrieval failed")
	})

	t.Run("Check - nominal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedPrincipal := "arn:aws:sts::123456789012:assumed-role/TestRole/session-name"

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAccessTokenInfo(gomock.Any(), "some-access-token").
			Return(&awsclient.AccessTokenInfo{Principal: expectedPrincipal}, nil)

		adapter := &AwsAdapter{
			AwsCredsClient: a,
			Logger:         zaptest.NewLogger(t),
		}

		credential := map[string]interface{}{
			"access_token": "some-access-token",
		}

		success, warnings, err := adapter.Check(context.Background(), credential)
		require.NoError(t, err)
		assert.Equal(t, []CheckCredentialsSuccess{{
			Id:          "principal",
			Description: "The IAM Principal identifier",
			Value:       expectedPrincipal,
		}}, success)
		assert.Equal(t, []Warning{}, warnings)
	})

	t.Run("Check - missing access token", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		adapter := &AwsAdapter{
			Logger: zaptest.NewLogger(t),
		}

		credential := map[string]interface{}{
			"other_field": "value",
		}

		_, _, err := adapter.Check(context.Background(), credential)
		require.EqualError(t, err, "access_token missing from credential")
	})

	t.Run("Check - get token info error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAccessTokenInfo(gomock.Any(), "some-access-token").
			Return(nil, errors.New("token info failed"))

		adapter := &AwsAdapter{
			AwsCredsClient: a,
			Logger:         zaptest.NewLogger(t),
		}

		credential := map[string]interface{}{
			"access_token": "some-access-token",
		}

		_, _, err := adapter.Check(context.Background(), credential)
		require.EqualError(t, err, "failed to get token info: token info failed")
	})

	t.Run("GetKubeconfig - nominal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		// Mock AWS credentials
		awsCredentials := awsclient.AWSCredentials{ //nolint:gosec
			AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
			Region:          testStsRegion,
			Expiration:      "2023-01-01T00:00:00Z",
		}
		credentialsJSON, _ := json.Marshal(awsCredentials) //nolint:gosec

		// Mock cluster info
		clusterInfo := &awsclient.ClusterInfo{
			Name:                     "test-cluster",
			Region:                   testStsRegion,
			Endpoint:                 "https://test-cluster.eks.us-west-2.amazonaws.com",
			CertificateAuthorityData: []byte("test-ca-data"),
		}

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: string(credentialsJSON),
			}), nil)
		a.EXPECT().GetClusterInfo(gomock.Any(), "test-cluster", testStsRegion, awsCredentials).
			Return(clusterInfo, nil)
		a.EXPECT().GenerateEksToken(gomock.Any(), "test-cluster", stsRegion, awsCredentials).
			Return("k8s-aws-v1.test-token", nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Name:   "test-cluster",
				Region: testStsRegion,
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		config, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.NoError(t, err)
		require.NotNil(t, config)

		// Verify the kubeconfig structure
		assert.Equal(t, "v1", config.APIVersion)
		assert.Equal(t, "Config", config.Kind)
		assert.Equal(t, "test-cluster", config.CurrentContext)

		// Verify cluster configuration
		cluster, exists := config.Clusters["test-cluster"]
		require.True(t, exists)
		assert.Equal(t, "https://test-cluster.eks.us-west-2.amazonaws.com", cluster.Server)
		assert.Equal(t, []byte("test-ca-data"), cluster.CertificateAuthorityData)

		// Verify context configuration
		context, exists := config.Contexts["test-cluster"]
		require.True(t, exists)
		assert.Equal(t, "test-cluster", context.Cluster)
		assert.Equal(t, "test-cluster", context.AuthInfo)

		// Verify auth info
		authInfo, exists := config.AuthInfos["test-cluster"]
		require.True(t, exists)
		assert.Equal(t, "k8s-aws-v1.test-token", authInfo.Token)
	})

	t.Run("GetKubeconfig - invalid configuration", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "", "some-org-some-runner", "some-id-token", stsRegion).
			Return(nil, errors.New("empty role ARN"))

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		// Invalid configuration - EKS with empty role ARN
		var eksCfg platformorchestratorcp.RunnerConfiguration
		invalidStsRegion := testStsRegion
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "", // Empty role ARN to trigger error
					StsRegion: &invalidStsRegion,
				},
			}})

		_, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get assume role token source")
	})

	t.Run("GetKubeconfig - missing access key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		// Mock AWS credentials missing access key
		awsCredentials := awsclient.AWSCredentials{
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
		}
		credentialsJSON, _ := json.Marshal(awsCredentials) //nolint:gosec

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: string(credentialsJSON),
			}), nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		_, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.EqualError(t, err, "failed to extract AWS credentials: access_key_id not found in AWS credentials")
	})

	t.Run("GetKubeconfig - missing secret key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		// Mock AWS credentials missing secret key
		awsCredentials := awsclient.AWSCredentials{ //nolint:gosec
			AccessKeyID:  "AKIAIOSFODNN7EXAMPLE",
			SessionToken: sessionToken,
		}
		credentialsJSON, _ := json.Marshal(awsCredentials) //nolint:gosec

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: string(credentialsJSON),
			}), nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		_, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.EqualError(t, err, "failed to extract AWS credentials: secret_access_key not found in AWS credentials")
	})

	t.Run("GetKubeconfig - missing session token", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		// Mock AWS credentials missing session token
		awsCredentials := awsclient.AWSCredentials{ //nolint:gosec
			AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: secretAccessKey,
		}
		credentialsJSON, _ := json.Marshal(awsCredentials) //nolint:gosec

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: string(credentialsJSON),
			}), nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		_, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.EqualError(t, err, "failed to extract AWS credentials: session_token not found in AWS credentials")
	})

	t.Run("GetKubeconfig - invalid credentials JSON", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stsRegion := testStsRegion

		a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
		a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
			Return(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: "invalid-json",
			}), nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
			Return("some-id-token", nil)

		adapter := &AwsAdapter{
			AwsCredsClient:            a,
			AwsTemporaryCredsProvider: &AwsTemporaryCredsProvider{OidcProvider: o, CredentialsClient: a},
			Logger:                    zaptest.NewLogger(t),
		}

		var eksCfg platformorchestratorcp.RunnerConfiguration
		_ = eksCfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerEksCluster{
				Auth: platformorchestratorcp.AwsTemporaryAuth{
					RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
					StsRegion: &stsRegion,
				},
			}})

		_, err := adapter.GetKubeconfig(context.Background(), "some-org", "some-runner", eksCfg)
		require.EqualError(t, err, "failed to extract AWS credentials: failed to parse AWS credentials from token: invalid character 'i' looking for beginning of value")
	})
}

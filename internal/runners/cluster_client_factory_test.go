package runners

import (
	"testing"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/cloud"
	cloud_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/cloud/mocks"
	mock_vault "github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault/mocks"
)

const (
	runnerCfgVaultPath    = "/platform-orchestrator/orgs/test-org/runners/default"
	runnerCfgVaultVersion = 1
	runnerId              = "default"
	orgId                 = "test-org"
)

func TestGetClusterClient_K8sRunner(t *testing.T) {
	t.Run("failed to read secret", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(nil, errors.New("failed to read a secret")).Times(1)
		cf := &clientFactory{}
		_, err := cf.GetClusterClient(t.Context(), platformorchestratorcp.InternalRunner{RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion}}, v)
		assert.EqualError(t, err, "failed to retrieve runner configuration sensitive data: failed to read a secret")
	})

	t.Run("missing auth", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		cfg := new(platformorchestratorcp.RunnerConfiguration)
		require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
				ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
					Server: "my-server",
				},
			},
			Job: platformorchestratorcp.K8sRunnerJobConfig{
				Namespace:      "platform-orchestrator-runner",
				ServiceAccount: "platform-orchestrator-runner",
			},
		}))
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{
			"cluster": map[string]interface{}{
				"auth": map[string]interface{}{},
			}}, nil).Times(1)
		cf := &clientFactory{}
		_, err := cf.GetClusterClient(t.Context(), platformorchestratorcp.InternalRunner{
			RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
			RunnerConfiguration:       *cfg,
		}, v)

		assert.EqualError(t, err, "kubernetes runner configuration must contain either a service account token or client cert and key data")
	})
}

func TestGetClusterClient_GCPRunner(t *testing.T) {
	cfg := new(platformorchestratorcp.RunnerConfiguration)
	_ = cfg.FromK8sGkeRunnerConfiguration(platformorchestratorcp.K8sGkeRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerGkeCluster{
			Name:      "my-cluster",
			ProjectId: "my-project",
			Location:  "my-location",
			Auth: platformorchestratorcp.K8sRunnerGcpTemporaryAuth{
				GcpServiceAccount: "my-service-account",
				GcpAudience:       "my-audience",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	})

	runner := platformorchestratorcp.InternalRunner{
		Id:                        runnerId,
		OrgId:                     orgId,
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(&clientcmdapi.Config{
			APIVersion: "v1",
			Kind:       "Config",
			Clusters: map[string]*clientcmdapi.Cluster{
				"my-cluster": {
					CertificateAuthorityData: []byte("DATA+OMITTED"),
					Server:                   "http://127.0.0.1",
				},
			},
			AuthInfos: map[string]*clientcmdapi.AuthInfo{
				"my-cluster": {
					Token: "token",
				},
			},
			Contexts: map[string]*clientcmdapi.Context{
				"my-cluster": {
					Cluster:  "my-cluster",
					AuthInfo: "my-cluster",
				},
			},
			CurrentContext: "my-cluster",
		}, nil)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesGke: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.NoError(t, err)
	})

	t.Run("failure - invalid config", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(&clientcmdapi.Config{}, nil)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesGke: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.ErrorContains(t, err, "error creating client config for runner default")
	})

	t.Run("failure - kubeconfig generation error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(nil, errors.New("some error"))
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesGke: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.ErrorContains(t, err, "some error")
	})
}

func TestGetClusterClient_EKSRunner(t *testing.T) {
	cfg := new(platformorchestratorcp.RunnerConfiguration)
	_ = cfg.FromK8sEksRunnerConfiguration(platformorchestratorcp.K8sEksRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerEksCluster{
			Name:   "my-eks-cluster",
			Region: "us-west-2",
			Auth: platformorchestratorcp.AwsTemporaryAuth{
				RoleArn: "arn:aws:iam::123456789012:role/TestRole",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	})

	runner := platformorchestratorcp.InternalRunner{
		Id:                        runnerId,
		OrgId:                     orgId,
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(&clientcmdapi.Config{
			APIVersion: "v1",
			Kind:       "Config",
			Clusters: map[string]*clientcmdapi.Cluster{
				"my-eks-cluster": {
					CertificateAuthorityData: []byte("DATA+OMITTED"),
					Server:                   "http://127.0.0.1",
				},
			},
			AuthInfos: map[string]*clientcmdapi.AuthInfo{
				"my-eks-cluster": { //nolint:gosec
					Token: "k8s-aws-v1.eyJleHAiOjE2MzQ1Njc4OTB9...",
				},
			},
			Contexts: map[string]*clientcmdapi.Context{
				"my-eks-cluster": {
					Cluster:  "my-eks-cluster",
					AuthInfo: "my-eks-cluster",
				},
			},
			CurrentContext: "my-eks-cluster",
		}, nil)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesEks: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.NoError(t, err)
	})

	t.Run("failure - invalid config", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(&clientcmdapi.Config{}, nil)
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesEks: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.ErrorContains(t, err, "error creating client config for EKS runner default")
	})

	t.Run("failure - kubeconfig generation error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		a := cloud_mocks.NewMockTypedAdapter(ctrl)
		a.EXPECT().GetKubeconfig(gomock.Any(), orgId, runnerId, *cfg).Return(nil, errors.New("some error"))
		v := mock_vault.NewMockVaultClientInterface(ctrl)
		v.EXPECT().ReadSecret(gomock.Any(), runnerCfgVaultPath, runnerCfgVaultVersion).Return(map[string]interface{}{}, nil).Times(1)

		cf := &clientFactory{
			adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
				platformorchestratorcp.RunnerTypeKubernetesEks: a,
			},
		}
		_, err := cf.GetClusterClient(t.Context(), runner, v)
		require.ErrorContains(t, err, "some error")
	})
}

package runners

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/gcp"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/cloud"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

type clientFactory struct {
	adaptors map[platformorchestratorcp.RunnerType]cloud.TypedAdapter
}

func NewClientFactory(hc *http.Client, oidcProvider oidc.Provider, logger *zap.Logger) kubernetes.ClientFactory {
	credsClient := aws.NewCredentialsClient()
	return &clientFactory{
		adaptors: map[platformorchestratorcp.RunnerType]cloud.TypedAdapter{
			platformorchestratorcp.RunnerTypeKubernetesGke: &cloud.GcpAdapter{
				GcpCredsClient: gcp.NewCredentialsClient(hc),
				OidcProvider:   oidcProvider,
				Logger:         logger,
			},
			platformorchestratorcp.RunnerTypeKubernetesEks: &cloud.AwsAdapter{
				AwsCredsClient:            credsClient,
				AwsTemporaryCredsProvider: &cloud.AwsTemporaryCredsProvider{OidcProvider: oidcProvider, CredentialsClient: credsClient},
				Logger:                    logger,
			},
		},
	}
}

func (cf *clientFactory) GetClusterClient(ctx context.Context, r platformorchestratorcp.InternalRunner, vlt vault.VaultClientInterface) (kubernetes.KubernetesInterface, error) {
	var k8sClient *k8s.Clientset
	var secretConfig map[string]interface{}
	var err error
	if r.RunnerConfigurationSecret != (platformorchestratorcp.ConfigurationSecret{}) {
		if secretConfig, err = vlt.ReadSecret(ctx, r.RunnerConfigurationSecret.Path, r.RunnerConfigurationSecret.Version); err != nil {
			return nil, errors.Wrap(err, "failed to retrieve runner configuration sensitive data")
		}
	}
	runnerType, _ := r.RunnerConfiguration.Discriminator()
	switch runnerType {
	case string(platformorchestratorcp.RunnerTypeKubernetes):
		if c, err := r.RunnerConfiguration.AsK8sRunnerConfiguration(); err != nil {
			return nil, errors.Wrap(err, "failed to convert runner configuration to kubernetes configuration")
		} else {
			if len(secretConfig) > 0 {
				secretConfigJson, _ := json.Marshal(secretConfig)
				var configToMerge platformorchestratorcp.RunnerConfiguration
				if err := json.Unmarshal(secretConfigJson, &configToMerge); err != nil {
					return nil, errors.Wrap(err, "failed to decode sensitive info to configure the runner")
				} else {
					if clusterSensitiveConfig, err := configToMerge.AsK8sRunnerConfiguration(); err != nil {
						return nil, errors.Wrap(err, "failed to decode sensitive cluster info in the runner configuration")
					} else {
						c.Cluster.Auth = clusterSensitiveConfig.Cluster.Auth
					}
				}
			}
			config := c.Cluster.ClusterData
			restConfig := &rest.Config{
				Host: config.Server,
				Proxy: func(*http.Request) (*url.URL, error) {
					if config.ProxyUrl != nil {
						proxyURL, err := url.Parse(*config.ProxyUrl)
						if err != nil {
							return nil, errors.Wrap(err, "failed to parse proxy URL")
						}
						return proxyURL, nil
					}
					return nil, nil
				},
			}

			if config.CertificateAuthorityData != "" {
				caData, err := base64.StdEncoding.DecodeString(config.CertificateAuthorityData)
				if err != nil {
					return nil, errors.Wrap(err, "failed to decode certificate authority data")
				}
				restConfig.CAData = caData
			}

			clusterAuth := c.Cluster.Auth
			if clusterAuth.ClientCertificateData != nil && clusterAuth.ClientKeyData != nil {
				certData, err := base64.StdEncoding.DecodeString(*clusterAuth.ClientCertificateData)
				if err != nil {
					return nil, errors.Wrap(err, "failed to decode client certificate data")
				}
				keyData, err := base64.StdEncoding.DecodeString(*clusterAuth.ClientKeyData)
				if err != nil {
					return nil, errors.Wrap(err, "failed to decode client key data")
				}
				restConfig.CertData = certData
				restConfig.KeyData = keyData
			} else if clusterAuth.ServiceAccountToken != nil {
				restConfig.BearerToken = *clusterAuth.ServiceAccountToken
			} else {
				return nil, usererrors.NewUserError("kubernetes runner configuration must contain either a service account token or client cert and key data")
			}
			if k8sClient, err = k8s.NewForConfig(restConfig); err != nil {
				return nil, errors.Wrap(err, "failed to create Kubernetes clientset")
			}
		}
	case string(platformorchestratorcp.RunnerTypeKubernetesGke):
		if adaptor, ok := cf.adaptors[platformorchestratorcp.RunnerTypeKubernetesGke]; !ok {
			return nil, errors.New(fmt.Sprintf("adapter for runner type %s not register", platformorchestratorcp.RunnerTypeKubernetesGke))
		} else if kubeconfig, err := adaptor.GetKubeconfig(ctx, r.OrgId, r.Id, r.RunnerConfiguration); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error getting kubeconfig for runner %s", r.Id))
		} else if restConfig, err := clientcmd.NewDefaultClientConfig(*kubeconfig, &clientcmd.ConfigOverrides{}).ClientConfig(); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error creating client config for runner %s", r.Id))
		} else if k8sClient, err = k8s.NewForConfig(restConfig); err != nil {
			return nil, errors.Wrap(err, "failed to create Kubernetes clientset")
		}
	case string(platformorchestratorcp.RunnerTypeKubernetesEks):
		if adaptor, ok := cf.adaptors[platformorchestratorcp.RunnerTypeKubernetesEks]; !ok {
			return nil, errors.New(fmt.Sprintf("adapter for runner type %s not register", platformorchestratorcp.RunnerTypeKubernetesEks))
		} else if kubeconfig, err := adaptor.GetKubeconfig(ctx, r.OrgId, r.Id, r.RunnerConfiguration); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error getting kubeconfig for EKS runner %s", r.Id))
		} else if restConfig, err := clientcmd.NewDefaultClientConfig(*kubeconfig, &clientcmd.ConfigOverrides{}).ClientConfig(); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error creating client config for EKS runner %s", r.Id))
		} else if k8sClient, err = k8s.NewForConfig(restConfig); err != nil {
			return nil, errors.Wrap(err, "failed to create Kubernetes clientset for EKS")
		}
	default:
		return nil, errors.Errorf("cluster configuration of type '%s' not supported", runnerType)
	}
	return kubernetes.NewKubernetesClient(k8sClient), nil
}

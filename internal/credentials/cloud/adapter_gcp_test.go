package cloud

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/gcp"
	gcp_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/gcp/mocks"
	oidc_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc/mocks"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

type errorTokenSource struct {
	Error error
}

func (e *errorTokenSource) Token() (*oauth2.Token, error) {
	return nil, e.Error
}

var _ oauth2.TokenSource = (*errorTokenSource)(nil)

func TestGcpAdapter(t *testing.T) {
	t.Run("nominal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		g := gcp_mocks.NewMockCredentialsClientInterface(ctrl)
		g.EXPECT().GetExternalAccountTokenSource(gomock.Any(), "some-audience", "some-service-account", "some-id-token", nil).
			Return(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "blah"}), nil)
		g.EXPECT().GetAccessTokenInfo(gomock.Any(), "blah").
			Return(&gcp.GetAccessTokenInfo{Principal: "user@example.com"}, nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "some-audience").
			Return("some-id-token", nil)

		a := &GcpAdapter{
			GcpCredsClient: g,
			OidcProvider:   o,
			Logger:         zaptest.NewLogger(t),
		}

		var gkeCfg platformorchestratorcp.RunnerConfiguration
		_ = gkeCfg.FromK8sGkeRunnerConfiguration(platformorchestratorcp.K8sGkeRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerGkeCluster{
				Auth: platformorchestratorcp.K8sRunnerGcpTemporaryAuth{
					GcpAudience:       "some-audience",
					GcpServiceAccount: "some-service-account",
				},
			}})

		cred, err := a.Exchange(context.Background(), "some-org", "some-runner", gkeCfg, ExchangeArgs{})
		require.NoError(t, err)
		f, w, err := a.Check(context.Background(), cred)
		require.NoError(t, err)
		assert.Equal(t, []CheckCredentialsSuccess{{Id: "principal", Description: "The IAM Principal identifier", Value: "user@example.com"}}, f)
		assert.Equal(t, []Warning{}, w)
	})

	t.Run("retrieve error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		g := gcp_mocks.NewMockCredentialsClientInterface(ctrl)
		g.EXPECT().GetExternalAccountTokenSource(gomock.Any(), "some-audience", "some-service-account", "some-id-token", nil).
			Return(&errorTokenSource{Error: &oauth2.RetrieveError{ErrorCode: "BANANA"}}, nil)

		o := oidc_mocks.NewMockProvider(ctrl)
		o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "some-audience").
			Return("some-id-token", nil)

		a := &GcpAdapter{
			GcpCredsClient: g,
			OidcProvider:   o,
			Logger:         zaptest.NewLogger(t),
		}
		var gkeCfg platformorchestratorcp.RunnerConfiguration
		_ = gkeCfg.FromK8sGkeRunnerConfiguration(platformorchestratorcp.K8sGkeRunnerConfiguration{
			Cluster: platformorchestratorcp.K8sRunnerGkeCluster{
				Auth: platformorchestratorcp.K8sRunnerGcpTemporaryAuth{
					GcpAudience:       "some-audience",
					GcpServiceAccount: "some-service-account",
				},
			},
		})

		_, err := a.Exchange(context.Background(), "some-org", "some-runner", gkeCfg, ExchangeArgs{})
		require.EqualError(t, err, "failed to fetch token: BANANA: unknown")
		ue := new(usererrors.UserError)
		assert.ErrorAs(t, err, &ue, "expect to be of type *errors.UserError")
	})

	t.Run("get kubeconfig", func(t *testing.T) {
		const projectId, location, clusterName, proxyUrl = "some-gcp-project", "europe-west3", "some-k8s-cluster", "https://some-proxy-url"
		const expectedAccessToken, expectedEndpoint, expectedCert, expectedPrivateEndpoint = "blah", "expected.endpoint", "DATA+OMITTED", "192.168.1.1"
		clusterId := fmt.Sprintf("gke_%s_%s_%s", projectId, location, clusterName)
		expectedCertDecoded, _ := base64.StdEncoding.DecodeString(expectedCert)
		gkeCluster := platformorchestratorcp.K8sRunnerGkeCluster{
			ProjectId: projectId,
			Location:  location,
			Name:      clusterName,
			Auth: platformorchestratorcp.K8sRunnerGcpTemporaryAuth{
				GcpAudience:       "some-audience",
				GcpServiceAccount: "some-service-account",
			},
		}

		tests := []struct {
			name               string
			useAgent           bool
			getCluster         []interface{}
			expectedKubeconfig *clientcmdapi.Config
			expectedErr        string
		}{
			{
				name: "success without agent",
				getCluster: []interface{}{&containerpb.Cluster{
					Id:       clusterId,
					Name:     clusterName,
					Endpoint: expectedEndpoint,
					MasterAuth: &containerpb.MasterAuth{
						ClusterCaCertificate: expectedCert,
					},
				}, nil},
				expectedKubeconfig: &clientcmdapi.Config{
					APIVersion: "v1",
					Kind:       "Config",
					Clusters: map[string]*clientcmdapi.Cluster{
						clusterId: {
							CertificateAuthorityData: expectedCertDecoded,
							Server:                   "https://" + expectedEndpoint,
						},
					},
					AuthInfos: map[string]*clientcmdapi.AuthInfo{
						clusterId: {
							Token: expectedAccessToken,
						},
					},
					Contexts: map[string]*clientcmdapi.Context{
						clusterId: {
							Cluster:  clusterId,
							AuthInfo: clusterId,
						},
					},
					CurrentContext: clusterId,
				},
			},
			{
				name:        "failure - get cluster error",
				getCluster:  []interface{}{nil, errors.New("gcp error")},
				expectedErr: "failed to get cluster description for runner \"some-runner\": gcp error",
			},
			{
				name:     "success with agent",
				useAgent: true,
				getCluster: []interface{}{&containerpb.Cluster{
					Id:       clusterId,
					Name:     clusterName,
					Endpoint: expectedEndpoint,
					MasterAuth: &containerpb.MasterAuth{
						ClusterCaCertificate: expectedCert,
					},
					ControlPlaneEndpointsConfig: &containerpb.ControlPlaneEndpointsConfig{
						IpEndpointsConfig: &containerpb.ControlPlaneEndpointsConfig_IPEndpointsConfig{
							PrivateEndpoint: expectedPrivateEndpoint,
						},
					},
				}, nil},
				expectedKubeconfig: &clientcmdapi.Config{
					APIVersion: "v1",
					Kind:       "Config",
					Clusters: map[string]*clientcmdapi.Cluster{
						clusterId: {
							CertificateAuthorityData: expectedCertDecoded,
							Server:                   "https://" + expectedPrivateEndpoint,
							ProxyURL:                 proxyUrl,
						},
					},
					AuthInfos: map[string]*clientcmdapi.AuthInfo{
						clusterId: {
							Token: expectedAccessToken,
						},
					},
					Contexts: map[string]*clientcmdapi.Context{
						clusterId: {
							Cluster:  clusterId,
							AuthInfo: clusterId,
						},
					},
					CurrentContext: clusterId,
				},
			},
			{
				name:     "failure with agent - no private endpoint configured",
				useAgent: true,
				getCluster: []interface{}{&containerpb.Cluster{
					Id:       clusterId,
					Name:     clusterName,
					Endpoint: expectedEndpoint,
					MasterAuth: &containerpb.MasterAuth{
						ClusterCaCertificate: expectedCert,
					},
				}, nil},
				expectedErr: "no private endpoint configured for GKE cluster",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				o := oidc_mocks.NewMockProvider(ctrl)
				o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "some-audience").
					Return("some-id-token", nil)

				g := gcp_mocks.NewMockCredentialsClientInterface(ctrl)
				g.EXPECT().GetExternalAccountTokenSource(gomock.Any(), "some-audience", "some-service-account", "some-id-token", nil).
					Return(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: expectedAccessToken}), nil)

				c := gcp_mocks.NewMockClusterManagerClientInterface(ctrl)
				c.EXPECT().GetCluster(gomock.Any(), &containerpb.GetClusterRequest{
					Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s", projectId, location, clusterName),
				}, gomock.Any()).Times(1).Return(test.getCluster...)
				c.EXPECT().Close().Times(1).Return(nil)

				a := &GcpAdapter{
					GcpCredsClient: g,
					OidcProvider:   o,
					Logger:         zaptest.NewLogger(t),
					NewClusterManagerClient: func(ctx context.Context, option ...option.ClientOption) (ClusterManagerClient, error) {
						return c, nil
					},
				}

				if test.useAgent {
					gkeCluster.ProxyUrl = ref.Ref(proxyUrl)
					gkeCluster.InternalIp = ref.Ref(true)
				} else {
					gkeCluster.ProxyUrl = nil
					gkeCluster.InternalIp = nil
				}
				var gkeCfg platformorchestratorcp.RunnerConfiguration
				_ = gkeCfg.FromK8sGkeRunnerConfiguration(platformorchestratorcp.K8sGkeRunnerConfiguration{
					Cluster: gkeCluster})
				kubeconfig, err := a.GetKubeconfig(context.Background(), "some-org", "some-runner", gkeCfg)
				if test.expectedErr == "" {
					require.NoError(t, err)
					assert.Equal(t, test.expectedKubeconfig, kubeconfig)
				} else {
					require.ErrorContains(t, err, test.expectedErr)
					ue := new(usererrors.UserError)
					assert.ErrorAs(t, err, &ue, "expect to be of type *errors.UserError")
				}
			})
		}
	})
}

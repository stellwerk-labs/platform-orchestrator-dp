package authenticator

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"

	"github.com/golang-jwt/jwt/v4"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/require"
)

func TestValidateJwtToken_success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cpClient := mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl)
	const orgId = "test-org"
	const runnerId = "test-runner"
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "platform-orchestrator-dp-test"})
	publicKey, privateKey := getEd25519KeyPair(t)
	var cfg platformorchestratorcp.RunnerConfiguration
	_ = cfg.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Type: platformorchestratorcp.RunnerTypeKubernetesAgent,
		Key:  publicKey,
	})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
			RunnerConfiguration:       cfg,
		},
	}, nil).Times(1)
	require.NoError(t, validateJwtToken(context.Background(), cpClient, orgId, runnerId, signJwt(t, privateKey, map[string]interface{}{
		"typ": "JWT",
		"alg": "EdDSA",
	})))
}

func TestValidateJwtToken_mismatchingKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cpClient := mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl)
	const orgId = "test-org"
	const runnerId = "test-runner"
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "platform-orchestrator-dp-test"})
	_, privateKey := getEd25519KeyPair(t)
	mismatchingPublicKey, _ := getEd25519KeyPair(t)
	var cfg platformorchestratorcp.RunnerConfiguration
	_ = cfg.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Type: platformorchestratorcp.RunnerTypeKubernetesAgent,
		Key:  mismatchingPublicKey,
	})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
			RunnerConfiguration:       cfg,
		},
	}, nil).Times(1)
	require.ErrorContains(t, validateJwtToken(context.Background(), cpClient, orgId, runnerId, signJwt(t, privateKey, map[string]interface{}{
		"typ": "JWT",
		"alg": "EdDSA",
	})), "failed to validate JWT token: ed25519: verification error")
}

func getEd25519KeyPair(t *testing.T) (string, []byte) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err, "failed to generate ed25519 key pair")

	derBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err, "failed to marshal public key to DER format")
	pem := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derBytes,
	})

	return string(pem), privateKey
}

func signJwt(t *testing.T, privateKey []byte, claims map[string]interface{}) string {
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims(claims))
	signedToken, err := token.SignedString(ed25519.PrivateKey(privateKey))
	require.NoError(t, err, "failed to sign JWT")
	return signedToken
}

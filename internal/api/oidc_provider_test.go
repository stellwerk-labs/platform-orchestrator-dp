package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mock_vault "github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault/mocks"
)

func testRSAPublicKey(t *testing.T) (string, string) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Fatalf("Error generating RSA private key: %v", err)
	}
	publicKey := &privateKey.PublicKey
	pubkeyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("Public key could not be serialized")
	}
	publicKeyPEM := &pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: pubkeyBytes,
	}
	shaSum := sha256.Sum256(publicKeyPEM.Bytes)
	return string(pem.EncodeToMemory(publicKeyPEM)), base64.RawURLEncoding.EncodeToString(shaSum[:])
}

func TestGetOpenidConfiguration(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	s.OidcIssuerUrl = "https://idtoken.platform-orchestrator.io"

	resp, err := s.GetOpenidConfiguration(context.Background(), GetOpenidConfigurationRequestObject{})
	require.NoError(t, err)

	expectedResponse := GetOpenidConfiguration200JSONResponse{
		Issuer:                 s.OidcIssuerUrl,
		JwksUri:                s.OidcIssuerUrl + "/.well-known/jwks",
		SubjectTypesSupported:  []string{"public", "pairwise"},
		ResponseTypesSupported: []string{"id_token"},
		ClaimsSupported: []string{
			"sub",
			"aud",
			"exp",
			"iat",
			"iss",
			"jti",
			"nbf",
		},
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
		ScopesSupported:                  []string{"openid"},
	}
	assert.Equal(t, expectedResponse, resp)
}

func TestGetOpenidConfiguration_NotConfigured(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	// No OIDC Issuer URL configured
	resp, err := s.GetOpenidConfiguration(context.Background(), GetOpenidConfigurationRequestObject{})
	require.NoError(t, err)
	require.Equal(t, GetOpenidConfiguration404JSONResponse{}, resp)
}

func TestGetJwks(t *testing.T) {
	pubKeyOne, _ := testRSAPublicKey(t)
	creationTimeOne, _ := time.Parse(time.RFC3339, "2025-05-01T15:00:00Z")
	pubKeyTwo, _ := testRSAPublicKey(t)
	creationTimeTwo := creationTimeOne.Add(1 * time.Hour)
	pubKeyThree, _ := testRSAPublicKey(t)
	creationTimeThree := creationTimeOne.Add(2 * time.Hour)

	mockReadKeyResponse := &vault.TransitKey{
		LatestVersion: 3,
		Keys: map[string]vault.TransitKeyVersion{
			"1": {
				PublicKey:    pubKeyOne,
				CreationTime: creationTimeOne,
			},
			"2": {
				PublicKey:    pubKeyTwo,
				CreationTime: creationTimeTwo,
			},
			"3": {
				PublicKey:    pubKeyThree,
				CreationTime: creationTimeThree,
			},
		},
		Name: "platform-orchestrator",
	}

	tests := []struct {
		name        string
		readResult  []interface{}
		expectedErr string
	}{
		{
			name:       "return jwk",
			readResult: []interface{}{mockReadKeyResponse, nil},
		},
		{
			name:       "create key and return jwk",
			readResult: []interface{}{nil, vault.ErrSecretNotFound},
		},
		{
			name:        "vault error",
			readResult:  []interface{}{nil, fmt.Errorf("vault error")},
			expectedErr: "vault error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()

			s.OidcIssuerUrl = "https://idtoken.platform-orchestrator.io"

			vaultMock := s.Vault.(*mock_vault.MockVaultClientInterface)

			vaultMock.EXPECT().ReadKey(gomock.Any(), oidc.KeyName).Times(1).Return(tt.readResult...)
			if tt.readResult[1] == vault.ErrSecretNotFound {
				vaultMock.EXPECT().CreateKey(gomock.Any(), oidc.KeyName, oidc.KeyAutoRotatePeriod).Times(1).Return(nil)
				vaultMock.EXPECT().ReadKey(gomock.Any(), oidc.KeyName).Times(1).Return(mockReadKeyResponse, nil)
			}

			resp, err := s.GetJwks(context.Background(), GetJwksRequestObject{})
			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				keys := resp.(GetJwks200JSONResponse).Keys
				require.Len(t, keys, 2)
				assert.Equal(t, fmt.Sprintf("%d", creationTimeThree.Unix()), keys[0].Kid)
				assert.Equal(t, "sig", keys[0].Use)
				assert.Equal(t, "RS256", keys[0].Alg)
				assert.Equal(t, "RSA", keys[0].Kty)

				assert.Equal(t, fmt.Sprintf("%d", creationTimeTwo.Unix()), keys[1].Kid)
			}
		})
	}
}

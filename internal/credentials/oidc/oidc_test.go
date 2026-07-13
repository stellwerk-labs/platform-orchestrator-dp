package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	mock_vault "github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault/mocks"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// Note: GetJwks function is tested in internal/api/oidc_provider.go

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

func TestCreateToken(t *testing.T) {
	issuerUrl := "https://oidc.platform-orchestrator.io"
	audience := "google.com"
	subject := "test-org+my-runner"
	pubKey, _ := testRSAPublicKey(t)

	mockReadKeyResponse := &vault.TransitKey{
		LatestVersion: 1,
		Keys: map[string]vault.TransitKeyVersion{
			"1": {
				PublicKey:    pubKey,
				CreationTime: time.Now().UTC(),
			},
		},
		Name: KeyName,
	}

	tests := []struct {
		name      string
		readKey   []interface{}
		expectErr string
	}{
		{
			name:    "success",
			readKey: []interface{}{mockReadKeyResponse, nil},
		},
		{
			name:      "success",
			readKey:   []interface{}{nil, errors.New("vault error")},
			expectErr: "vault error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			vaultMock := mock_vault.NewMockVaultClientInterface(ctrl)

			mockSignatureFromVault := map[string]interface{}{
				"signature": "vault:v1:signature",
			}

			var decodedInput string
			vaultMock.EXPECT().ReadKey(gomock.Any(), KeyName).Times(1).Return(tt.readKey...)
			if tt.expectErr == "" {
				vaultMock.EXPECT().SignData(gomock.Any(), KeyName, gomock.Any()).
					Times(1).
					DoAndReturn(func(ctx context.Context, key string, input string) (map[string]interface{}, error) {
						b, err := base64.StdEncoding.DecodeString(input)
						require.NoError(t, err)
						decodedInput = string(b)
						splitted := strings.Split(string(b), ".")
						assert.Len(t, splitted, 2)
						b, err = base64.RawURLEncoding.DecodeString(splitted[0])
						require.NoError(t, err)
						var headers map[string]interface{}
						require.NoError(t, json.Unmarshal(b, &headers))
						b, err = base64.RawURLEncoding.DecodeString(splitted[1])
						require.NoError(t, err)
						var claims map[string]interface{}
						require.NoError(t, json.Unmarshal(b, &claims))
						assert.Equal(t, "RS256", headers["alg"])
						assert.Equal(t, "JWT", headers["typ"])
						assert.Equal(t, issuerUrl, claims["iss"])
						assert.Equal(t, subject, claims["sub"])
						assert.Equal(t, audience, claims["aud"])
						return mockSignatureFromVault, nil
					})
			}

			testProvider := NewProvider(issuerUrl, vaultMock, ProviderOptions{})
			res, err := testProvider.CreateToken(context.Background(), subject, audience)
			if tt.expectErr == "" {
				require.NoError(t, err)
				assert.Equal(t, decodedInput+".signature", res)
			} else {
				assert.ErrorContains(t, err, tt.expectErr)
			}
		})
	}
}

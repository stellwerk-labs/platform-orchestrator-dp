package vault

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	vaultapi "github.com/hashicorp/vault/api"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	vaultToken  = "test-token"
	secretsPath = "path/to/secrets"
)

func NewTestVaultClient(t *testing.T, baseURL string, httpClient *http.Client) VaultClientInterface {
	client, err := vaultapi.NewClient(&vaultapi.Config{
		Address:    baseURL,
		HttpClient: httpClient,
	})
	require.NoError(t, err)
	client.SetToken(vaultToken)

	logger, _ := hlogger.NewTestLogger()
	return &vaultClient{
		path:        SecretPrefix,
		transitPath: TransitPrefix,
		client:      client,
		logger:      logger.Logger,
	}
}

func TestReadSecret(t *testing.T) {
	var tests = []struct {
		Name string

		VaultURL        string
		VaultStatusCode int
		VaultResponse   string

		Expected interface{}
	}{
		// Get: Success path
		//
		{
			Name:            "should read the secret from the vault",
			VaultStatusCode: http.StatusOK,
			VaultResponse: `{
								"data": {
									"data": { "key": "value" },
									"metadata": {
										"created_time": "2021-01-01T01:01:01.000Z",
										"deletion_time": "",
										"destroyed": false,
										"version": 0
									}
								}
							}`,
			Expected: map[string]interface{}{"key": "value"},
		},
		{
			Name:            "should handle the missing secret (not found)",
			VaultStatusCode: http.StatusNotFound,
			Expected:        ErrSecretNotFound,
		},

		// Get: Errors handling
		//
		{
			Name:     "should return error on bad vault URL",
			VaultURL: "wrong.domain/vault/path",
			Expected: errors.New("failed to read Vault secret"),
		},
		{
			Name:            "should return error on bad vault response JSON",
			VaultStatusCode: http.StatusOK,
			VaultResponse:   `{ not a valid JSON }`,
			Expected:        errors.New("invalid character 'n' looking for beginning of object key string"),
		},
		{
			Name:            "should return error on unexpected HTTP status code",
			VaultStatusCode: http.StatusNoContent,
			Expected:        errors.New("not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			fakeServer := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/secret/data/path/to/secrets":
							if r.Method != http.MethodGet {
								w.WriteHeader(http.StatusMethodNotAllowed)
								return
							}
							if r.Header.Get("X-Vault-Token") != vaultToken {
								w.WriteHeader(http.StatusUnauthorized)
								return
							}

							w.Header().Set("Content-Type", "application/json")
							w.WriteHeader(tt.VaultStatusCode)
							_, _ = w.Write([]byte(tt.VaultResponse))
							return
						}

						w.WriteHeader(http.StatusExpectationFailed)
					},
				),
			)
			defer fakeServer.Close()

			if tt.VaultURL == "" {
				tt.VaultURL = fakeServer.URL
			}

			vlt := NewTestVaultClient(t, tt.VaultURL, fakeServer.Client())

			res, err := vlt.ReadSecret(t.Context(), secretsPath, 1)

			if expErr, ok := tt.Expected.(error); ok {
				// On Error

				assert.ErrorContains(err, expErr.Error())
			} else if expRes, ok := tt.Expected.(map[string]interface{}); ok {
				// On Success

				require.NoError(t, err)
				assert.Equal(expRes, res)
			} else {
				t.Fatal("Wrong test expectation")
			}
		})
	}
}

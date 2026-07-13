package vault

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTransitKey = "test-key"
)

func TestReadKey(t *testing.T) {
	var tests = []struct {
		Name            string
		VaultStatusCode int
		VaultResponse   string
		Expected        interface{}
	}{
		{
			Name:            "should read the key from vault transit",
			VaultStatusCode: http.StatusOK,
			VaultResponse: `{
								"data": {
									"type": "rsa-4096",
									"latest_version": 1,
									"keys": {
										"1": {
											"public_key": "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDvMLuE5ck+lO7M3OXwcI6TXoIf\n+NJ4457htcKnVNqh4y1joznu4vrn9g0CHXs8eA0EFcpkV0p7+a+UzsSf2Qicsfh3\niQ4w/VHC/IBtNvQBPNwdhgDvrZTmDGZOh9Q0PkDY5ls6DFlc9hch+FEgSZTfvF4Y\nMlLSTno6b78w/snVCwIDAQAB\n-----END PUBLIC KEY-----",
											"creation_time": "2024-01-02T15:04:05Z"
										}
									},
									"name": "oidc-private-key"
								}
							}`,
			Expected: &TransitKey{
				Type:          "rsa-4096",
				LatestVersion: 1,
				Keys: map[string]TransitKeyVersion{
					"1": {
						PublicKey:    "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDvMLuE5ck+lO7M3OXwcI6TXoIf\n+NJ4457htcKnVNqh4y1joznu4vrn9g0CHXs8eA0EFcpkV0p7+a+UzsSf2Qicsfh3\niQ4w/VHC/IBtNvQBPNwdhgDvrZTmDGZOh9Q0PkDY5ls6DFlc9hch+FEgSZTfvF4Y\nMlLSTno6b78w/snVCwIDAQAB\n-----END PUBLIC KEY-----",
						CreationTime: time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC),
					},
				},
				Name: "oidc-private-key",
			},
		},
		{
			Name:            "should handle the missing key (not found)",
			VaultStatusCode: http.StatusNotFound,
			Expected:        ErrSecretNotFound,
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
			fakeServer := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/transit/keys/" + testTransitKey:
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

			vlt := NewTestVaultClient(t, fakeServer.URL, fakeServer.Client())

			res, err := vlt.ReadKey(t.Context(), testTransitKey)

			if expErr, ok := tt.Expected.(error); ok {
				// On Error
				assert.ErrorContains(t, err, expErr.Error())
			} else if expRes, ok := tt.Expected.(*TransitKey); ok {
				// On Success
				require.NoError(t, err)
				assert.Equal(t, expRes, res)
			} else {
				t.Fatal("Wrong test expectation")
			}
		})
	}
}

func TestCreateKey(t *testing.T) {
	var tests = []struct {
		Name            string
		VaultStatusCode int
		ExpectedErr     string
	}{
		{
			Name:            "should create the key in vault transit",
			VaultStatusCode: http.StatusOK,
		},
		{
			Name:            "should return error on unexpected HTTP status code",
			VaultStatusCode: http.StatusBadRequest,
			ExpectedErr:     "failed to create Vault key: `transit/keys/test-key`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeServer := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/transit/keys/" + testTransitKey:
							if r.Method != http.MethodPut {
								w.WriteHeader(http.StatusMethodNotAllowed)
								return
							}
							reqBody, err := io.ReadAll(r.Body)
							assert.NoError(t, err)
							var req map[string]interface{}
							assert.NoError(t, json.Unmarshal(reqBody, &req))
							assert.Equal(t, map[string]interface{}{
								"type":               RSA4096,
								"auto_rotate_period": "30d",
							}, req)

							if r.Header.Get("X-Vault-Token") != vaultToken {
								w.WriteHeader(http.StatusUnauthorized)
								return
							}
							w.WriteHeader(tt.VaultStatusCode)
							return
						}
						w.WriteHeader(http.StatusExpectationFailed)
					},
				),
			)
			defer fakeServer.Close()

			vlt := NewTestVaultClient(t, fakeServer.URL, fakeServer.Client())

			err := vlt.CreateKey(t.Context(), testTransitKey, "30d")

			if tt.ExpectedErr != "" {
				// On Error
				assert.ErrorContains(t, err, tt.ExpectedErr)
			} else {
				// On Success
				require.NoError(t, err)
			}
		})
	}
}

func TestSignData(t *testing.T) {
	var tests = []struct {
		Name            string
		VaultStatusCode int
		VaultResponse   string
		Expected        interface{}
	}{
		{
			Name:            "should sign data with the key in vault transit",
			VaultStatusCode: http.StatusOK,
			VaultResponse: `{
								"data": {
									"signature": "vault:v1:signature",
									"key_version": "1"
								}
							}`,
			Expected: map[string]interface{}{
				"signature":   "vault:v1:signature",
				"key_version": "1",
			},
		},
		{
			Name:            "should return error on unexpected HTTP status code",
			VaultStatusCode: http.StatusBadRequest,
			Expected:        errors.New("failed to sign data with Vault key: `transit/sign/test-key`"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeServer := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/transit/sign/" + testTransitKey:
							if r.Method != http.MethodPut {
								w.WriteHeader(http.StatusMethodNotAllowed)
								return
							}
							reqBody, err := io.ReadAll(r.Body)
							assert.NoError(t, err)
							var req map[string]interface{}
							assert.NoError(t, json.Unmarshal(reqBody, &req))
							assert.Equal(t, map[string]interface{}{
								"hash_algorithm":       JWTHashAlgorithm,
								"input":                "SOMEINPUT",
								"marshaling_algorithm": JWTMarshalingAlgorithm,
								"signature_algorithm":  JWTSignatureAlgorithm,
							}, req)

							if r.Header.Get("X-Vault-Token") != vaultToken {
								w.WriteHeader(http.StatusUnauthorized)
								return
							}
							w.WriteHeader(tt.VaultStatusCode)
							_, _ = w.Write([]byte(tt.VaultResponse))
							return
						}
						w.WriteHeader(http.StatusExpectationFailed)
					},
				),
			)
			defer fakeServer.Close()

			vlt := NewTestVaultClient(t, fakeServer.URL, fakeServer.Client())

			res, err := vlt.SignData(t.Context(), testTransitKey, `SOMEINPUT`)

			if expErr, ok := tt.Expected.(error); ok {
				// On Error
				assert.ErrorContains(t, err, expErr.Error())
			} else if expRes, ok := tt.Expected.(map[string]interface{}); ok {
				// On Success
				require.NoError(t, err)
				assert.Equal(t, expRes, res)
			} else {
				t.Fatal("Wrong test expectation")
			}
		})
	}
}

func TestVerifySignature(t *testing.T) {
	var tests = []struct {
		Name            string
		VaultStatusCode int
		VaultResponse   string
		Expected        interface{}
	}{
		{
			Name:            "should verify signature in vault transit",
			VaultStatusCode: http.StatusOK,
			VaultResponse: `{
								"data": {
									"valid": true
								}
							}`,
			Expected: true,
		},
		{
			Name:            "should not verify signature in vault transit",
			VaultStatusCode: http.StatusOK,
			VaultResponse: `{
								"data": {
									"valid": false
								}
							}`,
			Expected: false,
		},
		{
			Name:            "should return error on unexpected HTTP status code",
			VaultStatusCode: http.StatusBadRequest,
			Expected:        errors.New("failed to verify data with Vault key: `transit/verify/test-key`"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeServer := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/transit/verify/" + testTransitKey:
							if r.Method != http.MethodPut {
								w.WriteHeader(http.StatusMethodNotAllowed)
								return
							}
							reqBody, err := io.ReadAll(r.Body)
							assert.NoError(t, err)
							var req map[string]interface{}
							assert.NoError(t, json.Unmarshal(reqBody, &req))
							assert.Equal(t, map[string]interface{}{
								"hash_algorithm":       JWTHashAlgorithm,
								"input":                "SOMEINPUT",
								"marshaling_algorithm": JWTMarshalingAlgorithm,
								"signature_algorithm":  JWTSignatureAlgorithm,
								"signature":            "signature",
							}, req)

							if r.Header.Get("X-Vault-Token") != vaultToken {
								w.WriteHeader(http.StatusUnauthorized)
								return
							}
							w.WriteHeader(tt.VaultStatusCode)
							_, _ = w.Write([]byte(tt.VaultResponse))
							return
						}
						w.WriteHeader(http.StatusExpectationFailed)
					},
				),
			)
			defer fakeServer.Close()

			vlt := NewTestVaultClient(t, fakeServer.URL, fakeServer.Client())

			res, err := vlt.VerifySignature(t.Context(), testTransitKey, "SOMEINPUT", "signature")

			if expErr, ok := tt.Expected.(error); ok {
				// On Error
				assert.ErrorContains(t, err, expErr.Error())
			} else if expRes, ok := tt.Expected.(bool); ok {
				// On Success
				require.NoError(t, err)
				assert.Equal(t, expRes, res)
			} else {
				t.Fatal("Wrong test expectation")
			}
		})
	}
}

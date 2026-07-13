package vault

import (
	"context"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

const (
	RSA4096                  = "rsa-4096"
	JWTHashAlgorithm         = "sha2-256"
	JWTSignatureAlgorithm    = "pkcs1v15"
	JWTMarshalingAlgorithm   = "jws"
	inputField               = "input"
	hashAlgorithmField       = "hash_algorithm"
	signatureAlgorithmField  = "signature_algorithm"
	marshalingAlgorithmField = "marshaling_algorithm"
	signatureField           = "signature"
)

// TransitKeyVersion represents the version of the key in Vault Transit
type TransitKeyVersion struct {
	CreationTime time.Time `json:"creation_time"`
	Name         string    `json:"name"`
	PublicKey    string    `json:"public_key"`
}

// TransitKey represents the key stored in Vault Transit, it includes only fields used in the service
type TransitKey struct {
	Name          string                       `json:"name"`
	Type          string                       `json:"type"`
	Keys          map[string]TransitKeyVersion `json:"keys"`
	LatestVersion int                          `json:"latest_version"`
}

func mapToStruct[T any](m map[string]interface{}) (*T, error) {
	var res *T
	if jsonBytes, err := json.Marshal(m); err != nil {
		return nil, err
	} else if err = json.Unmarshal(jsonBytes, &res); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}

func (vlt *vaultClient) GetKeyPath(keyName string) string {
	return filepath.Join(vlt.transitPath, "keys", keyName)
}

func (vlt *vaultClient) GetSignPath(keyName string) string {
	return filepath.Join(vlt.transitPath, "sign", keyName)
}

func (vlt *vaultClient) GetVerifyPath(keyName string) string {
	return filepath.Join(vlt.transitPath, "verify", keyName)
}

// ReadKey fetches a key from Transit engine in Vault
func (vlt *vaultClient) ReadKey(ctx context.Context, keyName string) (*TransitKey, error) {
	fullPath := vlt.GetKeyPath(keyName)
	secret, err := vlt.client.Logical().ReadWithContext(ctx, fullPath)
	if err != nil {
		if errors.Is(err, api.ErrSecretNotFound) {
			return nil, ErrSecretNotFound
		}
		return nil, errors.Wrapf(err, "failed to read Vault secret `%s`", fullPath)
	}
	if secret == nil {
		return nil, ErrSecretNotFound
	}
	if key, err := mapToStruct[TransitKey](secret.Data); err != nil {
		return nil, errors.Wrapf(err, "failed to convert Vault secret `%s`", fullPath)
	} else {
		return key, nil
	}
}

// CreateKey creates a key in Transit engine in Vault
func (vlt *vaultClient) CreateKey(ctx context.Context, keyName, autoRotatePeriod string) error {
	data := map[string]interface{}{
		"type": RSA4096,
	}
	if autoRotatePeriod != "" {
		data["auto_rotate_period"] = autoRotatePeriod
	}
	_, err := vlt.client.Logical().WriteWithContext(ctx, vlt.GetKeyPath(keyName), data)
	if err != nil {
		return errors.Wrapf(err, "failed to create Vault key: `%s`", vlt.GetKeyPath(keyName))
	}
	return nil
}

// SignData signs a data with a key in Transit engine in Vault
func (vlt *vaultClient) SignData(ctx context.Context, key string, input string) (map[string]interface{}, error) {
	data := map[string]interface{}{
		inputField:               input,
		hashAlgorithmField:       JWTHashAlgorithm,
		signatureAlgorithmField:  JWTSignatureAlgorithm,
		marshalingAlgorithmField: JWTMarshalingAlgorithm,
	}
	secret, err := vlt.client.Logical().WriteWithContext(ctx, vlt.GetSignPath(key), data)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to sign data with Vault key: `%s`", vlt.GetSignPath(key))
	}
	return secret.Data, nil
}

// VerifySignature verify singed data with a key in Transit engine in Vault
func (vlt *vaultClient) VerifySignature(ctx context.Context, key, input, signature string) (bool, error) {
	data := map[string]interface{}{
		inputField:               input,
		hashAlgorithmField:       JWTHashAlgorithm,
		signatureAlgorithmField:  JWTSignatureAlgorithm,
		marshalingAlgorithmField: JWTMarshalingAlgorithm,
		signatureField:           signature,
	}
	secret, err := vlt.client.Logical().WriteWithContext(ctx, vlt.GetVerifyPath(key), data)
	if err != nil {
		return false, errors.Wrapf(err, "failed to verify data with Vault key: `%s`", vlt.GetVerifyPath(key))
	}

	if valid, ok := secret.Data["valid"].(bool); !ok {
		return false, errors.Wrapf(err, "verifying response data should contain boolean `valid` field, using key: `%s`", vlt.GetVerifyPath(key))
	} else {
		return valid, nil
	}
}

package vault

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

const (
	RSA4096                  = "rsa-4096"
	AES256GCM96              = "aes256-gcm96"
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

// UnmarshalJSON accepts both Vault representations. Asymmetric Transit keys
// expose metadata objects, while symmetric keys expose an opaque numeric key
// version. Callers only need the latter to prove that the key exists.
func (v *TransitKeyVersion) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] != '{' {
		var version json.Number
		return json.Unmarshal(data, &version)
	}
	type transitKeyVersion TransitKeyVersion
	return json.Unmarshal(data, (*transitKeyVersion)(v))
}

// TransitKey represents the key stored in Vault Transit, it includes only fields used in the service
type TransitKey struct {
	Name          string                       `json:"name"`
	Type          string                       `json:"type"`
	Exportable    bool                         `json:"exportable"`
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

// CreateEncryptionKey creates a non-exportable symmetric Transit key. Vault
// retains prior rotated key versions so existing ciphertext remains readable.
func (vlt *vaultClient) CreateEncryptionKey(ctx context.Context, keyName, autoRotatePeriod string) error {
	data := map[string]interface{}{"type": AES256GCM96}
	if autoRotatePeriod != "" {
		data["auto_rotate_period"] = autoRotatePeriod
	}
	if _, err := vlt.client.Logical().WriteWithContext(ctx, vlt.GetKeyPath(keyName), data); err != nil {
		return errors.Wrapf(err, "failed to create Vault encryption key: `%s`", vlt.GetKeyPath(keyName))
	}
	return nil
}

func (vlt *vaultClient) Encrypt(ctx context.Context, keyName string, plaintext []byte) (string, error) {
	path := filepath.Join(vlt.transitPath, "encrypt", keyName)
	secret, err := vlt.client.Logical().WriteWithContext(ctx, path, map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(plaintext),
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to encrypt data with Vault key: `%s`", path)
	}
	if secret == nil {
		return "", errors.Errorf("Vault encryption response was empty for key `%s`", keyName)
	}
	ciphertext, ok := secret.Data["ciphertext"].(string)
	if !ok || ciphertext == "" {
		return "", errors.Errorf("Vault encryption response did not contain ciphertext for key `%s`", keyName)
	}
	return ciphertext, nil
}

func (vlt *vaultClient) Decrypt(ctx context.Context, keyName, ciphertext string) ([]byte, error) {
	path := filepath.Join(vlt.transitPath, "decrypt", keyName)
	secret, err := vlt.client.Logical().WriteWithContext(ctx, path, map[string]interface{}{"ciphertext": ciphertext})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decrypt data with Vault key: `%s`", path)
	}
	if secret == nil {
		return nil, errors.Errorf("Vault decryption response was empty for key `%s`", keyName)
	}
	plaintext, ok := secret.Data["plaintext"].(string)
	if !ok || plaintext == "" {
		return nil, errors.Errorf("Vault decryption response did not contain plaintext for key `%s`", keyName)
	}
	decoded, err := base64.StdEncoding.DecodeString(plaintext)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode Vault plaintext")
	}
	return decoded, nil
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

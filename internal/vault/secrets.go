package vault

import (
	"context"

	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

// ReadSecret fetches the latest version of a path from the KV store in vault
func (vlt *vaultClient) ReadSecret(ctx context.Context, path string, version int) (map[string]interface{}, error) {
	if secret, err := vlt.client.KVv2(vlt.path).GetVersion(ctx, path, version); err != nil {
		if errors.Is(err, api.ErrSecretNotFound) {
			return nil, ErrSecretNotFound
		}
		return nil, errors.Wrapf(err, "failed to read Vault secret `%s`", path)
	} else if secret == nil {
		return nil, ErrSecretNotFound
	} else {
		return secret.Data, nil
	}
}

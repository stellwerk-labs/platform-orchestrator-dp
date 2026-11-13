//go:build integration

package costestimation

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCredentialCustodyAndRotationWithRealVault(t *testing.T) {
	address := os.Getenv("COST_POLICY_TEST_VAULT_ADDR")
	if address == "" {
		t.Skip("requires disposable Vault and PostgreSQL fixtures")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	config := vaultapi.DefaultConfig()
	config.Address = address
	client, err := vaultapi.NewClient(config)
	require.NoError(t, err)
	client.SetToken(os.Getenv("COST_POLICY_TEST_VAULT_TOKEN"))
	vlt := vault.NewVaultClient(client, zap.NewNop())
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	schema := "cost_credential_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer func() { _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); require.NoError(t, err) }()
	db, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	_, err = Resolve(ctx, db, vlt)
	require.ErrorIs(t, err, ErrCredentialUnavailable)
	for i, key := range []string{"fixture-key-first", "fixture-key-rotated"} {
		ciphertext, err := Encrypt(ctx, vlt, key)
		require.NoError(t, err)
		require.NotContains(t, ciphertext, key)
		require.True(t, strings.HasPrefix(ciphertext, "vault:v"))
		_, err = model.UpdateCostPolicy(ctx, db, uuid.New(), int64(i+1), true, &ciphertext)
		require.NoError(t, err)
		actual, err := Resolve(ctx, db, vlt)
		require.NoError(t, err)
		require.Equal(t, key, actual)
		stored, err := model.ReadCostAPIKeyCiphertext(ctx, db)
		require.NoError(t, err)
		require.Equal(t, ciphertext, stored)
	}
	key, err := vlt.ReadKey(ctx, transitKey)
	require.NoError(t, err)
	require.Equal(t, vault.AES256GCM96, key.Type)
	require.False(t, key.Exportable)
	_, err = model.UpdateCostPolicy(ctx, db, uuid.New(), 3, false, ref.Ref(""))
	require.NoError(t, err)
	_, err = Resolve(ctx, db, vlt)
	require.ErrorIs(t, err, ErrCredentialUnavailable)
	config.Address = "http://127.0.0.1:1"
	unavailable, err := vaultapi.NewClient(config)
	require.NoError(t, err)
	unavailable.SetMaxRetries(0)
	_, err = Encrypt(ctx, vault.NewVaultClient(unavailable, zap.NewNop()), "never-expose-this-key")
	require.ErrorIs(t, err, ErrCredentialUnavailable)
	require.NotContains(t, err.Error(), "never-expose")
}

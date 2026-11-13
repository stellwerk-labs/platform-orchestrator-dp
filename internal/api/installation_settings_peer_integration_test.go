//go:build integration

package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Optional cross-repository fixture. Its caller supplies a real IAM and a
// disposable database, never a simulated authorization server.
func TestInstallationSettingsPeer(t *testing.T) {
	ready := os.Getenv("PLATFORM_SETTINGS_PEER_READY")
	if ready == "" {
		t.Skip("optional real IAM/Console qualification peer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	schema := "settings_peer_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer func() { _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); require.NoError(t, err) }()
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" options='-c search_path="+schema+"'")
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	certificate, err := os.ReadFile(os.Getenv("PLATFORM_SETTINGS_CA_FILE"))
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certificate))
	client, err := iamapi.NewClientWithResponses(os.Getenv("PLATFORM_SETTINGS_IAM_URL"), iamapi.WithHTTPClient(&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}))
	require.NoError(t, err)
	server := &Server{Database: store, IamClient: client, Logger: zap.NewNop(), UiHostUrl: os.Getenv("PLATFORM_SETTINGS_IAM_URL")}
	if address := os.Getenv("COST_POLICY_TEST_VAULT_ADDR"); address != "" {
		config := vaultapi.DefaultConfig()
		config.Address = address
		client, err := vaultapi.NewClient(config)
		require.NoError(t, err)
		client.SetToken(os.Getenv("COST_POLICY_TEST_VAULT_TOKEN"))
		server.Vault = vault.NewVaultClient(client, zap.NewNop())
	}
	e := echo.New()
	server.MapRoutes(e)
	peer := httptest.NewServer(e)
	defer peer.Close()
	require.NoError(t, os.WriteFile(ready, []byte(peer.URL), 0600))
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("real settings peer was not stopped")
		case <-ticker.C:
			if _, err := os.Stat(ready + ".stop"); err == nil {
				return
			}
		}
	}
}

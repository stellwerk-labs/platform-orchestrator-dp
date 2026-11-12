//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Optional peer for the cross-repository maintenance qualification. Only the
// disposable test listener exposes the control route; it calls the real model.
func TestMaintenanceDataPlanePeer(t *testing.T) {
	ready := os.Getenv("MAINTENANCE_PEER_READY")
	if ready == "" {
		t.Skip("optional cross-repository qualification peer")
	}
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	defer admin.Close() //nolint:errcheck
	schema := "maintenance_dp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE") //nolint:errcheck
	store, err := model.NewDatabaser(t.Context(), zap.NewNop(), connection+" options='-c search_path="+schema+"'")
	require.NoError(t, err)
	defer store.Close() //nolint:errcheck
	_, err = model.UpdateInstallationMaintenance(t.Context(), store, uuid.New(), 1, true)
	require.NoError(t, err)
	app := echo.New()
	s := Server{Database: store}
	app.GET("/internal/installation-maintenance", s.getInternalInstallationMaintenance)
	app.PUT("/test/maintenance", func(c echo.Context) error {
		var body struct {
			Enabled  bool  `json:"enabled"`
			Revision int64 `json:"revision"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
			return err
		}
		value, err := model.UpdateInstallationMaintenance(c.Request().Context(), store, uuid.New(), body.Revision, body.Enabled)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, value)
	})
	peer := httptest.NewServer(app)
	defer peer.Close()
	require.NoError(t, os.WriteFile(ready, []byte(peer.URL), 0600))
	for until := time.Now().Add(100 * time.Second); time.Now().Before(until); {
		if _, err := os.Stat(os.Getenv("MAINTENANCE_PEER_STOP")); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("maintenance peer exceeded its lifetime")
}

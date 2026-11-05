//go:build integration

package model

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestObservationRecoveryMigrationRetainsNegativeReceiptsAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	_, _, scoped := migrationTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	legacy, err := sql.Open("postgres", scoped)
	require.NoError(t, err)
	defer func() { _ = legacy.Close() }()
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source, goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.UpTo(ctx, 38)
	require.NoError(t, err)
	request := observation.ObservationRequest{RequestId: uuid.New(), GrantId: uuid.New(), GrantRevision: 1, ProjectId: "p", EnvId: "e", ProjectUuid: uuid.New(), EnvironmentUuid: uuid.New(), Occurrence: time.Now(), AdmissionDeadline: time.Now().Add(time.Minute)}
	actor := uuid.New()
	for range 2 {
		store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
		require.NoError(t, err)
		resolution, err := ResolveObservationRequest(ctx, store, "migration-org", "dev.example.plugin", actor, request)
		require.NoError(t, err)
		require.Equal(t, observation.ObservationResolutionDispositionNotStarted, resolution.Disposition)
		_, _, err = TryStartObservation(ctx, store, "migration-org", "dev.example.plugin", actor, request, "runner", 2, true)
		require.ErrorIs(t, err, ErrObservationRequestSealed)
		require.NoError(t, store.Close())
	}
	var count int
	require.NoError(t, legacy.QueryRowContext(ctx, `SELECT count(*) FROM environment_observation_request_seals`).Scan(&count))
	require.Equal(t, 1, count)
	_, err = provider.DownTo(ctx, 38)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot discard sealed observation requests")
}

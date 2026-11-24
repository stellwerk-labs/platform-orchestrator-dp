//go:build integration

package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMigrationHistoryReconciliation(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")

	t.Run("fresh database installs only generic external operation schema", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		admin, schema, scopedConnection := migrationTestDatabase(t, ctx, connection)

		store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		requireColumn(t, ctx, admin, schema, "deployments", "module_version_request_digest")
		requireTableAbsent(t, ctx, admin, schema, "rollout_encryption_material")
		requireTableAbsent(t, ctx, admin, schema, "rollout_deployment_bases")
		requireTablePresent(t, ctx, admin, schema, "environment_deployment_leases")
		requireTablePresent(t, ctx, admin, schema, "external_deployment_plan_snapshots")
		requireColumn(t, ctx, admin, schema, "external_deployment_plan_snapshots", "explicit_targets")
		requireTablePresent(t, ctx, admin, schema, "external_operation_material")
		requireColumn(t, ctx, admin, schema, "external_operation_deployments", "plan_snapshot_id")
		requireColumn(t, ctx, admin, schema, "external_operation_deployments", "base_deployment_id")
		requireTablePresent(t, ctx, admin, schema, "deployment_admission_receipts")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")
		requireTablePresent(t, ctx, admin, schema, "deployment_outcome_acknowledgements")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "native_admission_watermark")
		requireTablePresent(t, ctx, admin, schema, "deployment_pipeline_controls")
	})

	t.Run("exercised version 28 receives additive lifecycle evidence", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		admin, schema, scopedConnection := migrationTestDatabase(t, ctx, connection)
		legacy, err := sql.Open("postgres", scopedConnection)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, legacy.Close()) })

		source, err := fs.Sub(embedMigrations, "migrations")
		require.NoError(t, err)
		provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source,
			goose.WithDisableGlobalRegistry(true),
			goose.WithGoMigrations(goose.NewGoMigration(2,
				&goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01},
				&goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
		require.NoError(t, err)
		_, err = provider.UpTo(ctx, 28)
		require.NoError(t, err)
		requireColumnAbsent(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumnAbsent(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")

		_, err = legacy.ExecContext(ctx, `INSERT INTO deployment_admission_receipts
			(org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,core_epoch,disposition,deployment_id,terminal_outcome)
			VALUES ('migration-repair',$1,$2,$3,$4,$5,'deploy','{}'::jsonb,1,'committed',$6,'succeeded')`,
			uuid.New(), uuid.New(), uuid.New(), uuid.New(), strings.Repeat("a", 64), uuid.New())
		require.NoError(t, err)

		_, err = provider.Up(ctx)
		require.NoError(t, err)
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")
		var carryForward, confirmed string
		require.NoError(t, legacy.QueryRowContext(ctx,
			`SELECT carry_forward_module_versions::text,confirmed_restricted_version_uuids::text FROM deployment_admission_receipts`,
		).Scan(&carryForward, &confirmed))
		require.Equal(t, "{}", carryForward)
		require.Equal(t, "[]", confirmed)
	})

	for _, retained := range []struct {
		name    string
		version int64
	}{
		{"retained version 54 installs 55 then 56", 54},
		{"retained version 55 installs 56", 55},
		{"owned version 56 without 55 refuses startup", 56},
	} {
		t.Run(retained.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			admin, schema, scopedConnection := migrationTestDatabase(t, ctx, connection)
			db := schemaConnection(t, scopedConnection)
			source, err := fs.Sub(embedMigrations, "migrations")
			require.NoError(t, err)
			options := []goose.ProviderOption{
				goose.WithDisableGlobalRegistry(true),
				goose.WithGoMigrations(goose.NewGoMigration(2,
					&goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01},
					&goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})),
			}
			if retained.version == 56 {
				// Reproduce the unreleased controls schema using only this owned fixture.
				options = append(options, goose.WithExcludeVersions([]int64{55}))
			}
			provider, err := goose.NewProvider(goose.DialectPostgres, db, source, options...)
			require.NoError(t, err)
			_, err = provider.UpTo(ctx, retained.version)
			require.NoError(t, err)
			applied := appliedMigrations(t, ctx, db)
			require.Equal(t, retained.version, applied[len(applied)-1])
			if retained.version == 55 {
				requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "native_admission_watermark")
			} else {
				requireColumnAbsent(t, ctx, admin, schema, "deployment_admission_receipts", "native_admission_watermark")
				require.NotContains(t, applied, int64(55))
			}

			target, err := json.Marshal(testNativeTarget())
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `INSERT INTO deployment_admission_receipts
				(org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,
				 carry_forward_module_versions,confirmed_restricted_version_uuids,core_epoch,disposition,deployment_id,
				 terminal_outcome,core_settled_at,native_target)
				VALUES ('migration-retained',$1,$2,$3,$4,$5,'deploy','{}','{}','[]',1,'committed',$6,'succeeded',now(),$7)`,
				uuid.New(), uuid.New(), uuid.New(), uuid.New(), strings.Repeat("b", 64), uuid.New(), string(target))
			require.NoError(t, err)
			if retained.version == 55 {
				_, err = db.ExecContext(ctx, `UPDATE deployment_admission_receipts
					SET mode='plan_only',native_admission_watermark='{"admission_id":"33333333-3333-4333-8333-333333333333","epoch":1}'`)
				require.NoError(t, err)
			}
			const receiptQuery = `SELECT (jsonb_build_object('native_admission_watermark',NULL) || to_jsonb(receipt))::text
				FROM deployment_admission_receipts receipt`
			var receiptBefore string
			require.NoError(t, db.QueryRowContext(ctx, receiptQuery).Scan(&receiptBefore))

			if retained.version == 56 {
				_, err = db.ExecContext(ctx, `INSERT INTO deployment_pipeline_controls
					(id,org_id,deployment_id,environment_id,actor_id,client_key,intent_digest,request_digest,execution_digest,
					 runner_uuid,action,command,created_at,expires_at)
					VALUES ($1,'migration-retained',$2,$3,$4,'retained-control',$5,$5,$5,$6,'cancel',$7,now(),now()+interval '1 hour')`,
					uuid.New(), uuid.New(), uuid.New(), uuid.New(), strings.Repeat("c", 64), uuid.New(), []byte("retained control bytes"))
				require.NoError(t, err)
				const snapshotQuery = `SELECT jsonb_build_object(
					'columns',(SELECT jsonb_agg(to_jsonb(c) ORDER BY table_name,ordinal_position)
						FROM information_schema.columns c WHERE table_schema=current_schema()),
					'constraints',(SELECT jsonb_agg(jsonb_build_object('table',r.relname,'name',c.conname,
						'definition',pg_get_constraintdef(c.oid)) ORDER BY r.relname,c.conname)
						FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
						JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=current_schema()),
					'indexes',(SELECT jsonb_agg(to_jsonb(i) ORDER BY indexname)
						FROM pg_indexes i WHERE schemaname=current_schema()),
					'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY admission_id) FROM deployment_admission_receipts r),
					'controls',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM deployment_pipeline_controls c),
					'history',(SELECT jsonb_agg(to_jsonb(v) ORDER BY id) FROM goose_db_version v)
				)::text`
				var before, after string
				require.NoError(t, db.QueryRowContext(ctx, snapshotQuery).Scan(&before))

				_, err = NewDatabaser(ctx, zap.NewNop(), scopedConnection)
				require.ErrorContains(t, err, "found 1 missing migrations before current version 56")
				require.ErrorContains(t, err, "version 55: migrations/000055_native_admission_watermark.sql")
				require.NoError(t, db.QueryRowContext(ctx, snapshotQuery).Scan(&after))
				require.Equal(t, before, after, "refused startup must preserve schema, retained receipts/controls and exact migration history")
				return
			}

			requireTableAbsent(t, ctx, admin, schema, "deployment_pipeline_controls")
			requireStarts(t, ctx, scopedConnection)
			requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "native_admission_watermark")
			requireTablePresent(t, ctx, admin, schema, "deployment_pipeline_controls")
			var receiptAfter string
			require.NoError(t, db.QueryRowContext(ctx, receiptQuery).Scan(&receiptAfter))
			require.Equal(t, receiptBefore, receiptAfter, "upgrade must preserve retained receipt and Native evidence")
			if retained.version == 54 {
				applied = append(applied, 55)
			}
			require.Equal(t, append(applied, 56), appliedMigrations(t, ctx, db))
		})
	}
}

func migrationTestDatabase(t *testing.T, ctx context.Context, connection string) (*sql.DB, string, string) {
	t.Helper()
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "migration_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	return admin, schema, connection + " search_path=" + schema
}

func requireColumn(t *testing.T, ctx context.Context, db *sql.DB, schema, table, column string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
		)
	`, schema, table, column).Scan(&exists))
	require.True(t, exists, "%s.%s.%s must exist", schema, table, column)
}

func requireColumnAbsent(t *testing.T, ctx context.Context, db *sql.DB, schema, table, column string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
		)
	`, schema, table, column).Scan(&exists))
	require.False(t, exists, "%s.%s.%s must be absent", schema, table, column)
}

func requireTableAbsent(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema=$1 AND table_name=$2
		)
	`, schema, table).Scan(&exists))
	require.False(t, exists, "%s.%s must not exist", schema, table)
}

func requireTablePresent(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema=$1 AND table_name=$2
		)
	`, schema, table).Scan(&exists))
	require.True(t, exists, "%s.%s must exist", schema, table)
}

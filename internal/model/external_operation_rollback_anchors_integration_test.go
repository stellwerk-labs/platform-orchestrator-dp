//go:build integration

package model

import (
	"context"
	"database/sql"
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

func TestRollbackAnchorSerializesFirstAdmissionAndRejectsScopeDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	anchor := ExternalOperationRollbackAnchor{OrgID: "anchor-org", PluginID: "dev.example.rollouts", OperationRef: uuid.NewString(),
		ProjectID: "project", EnvID: "env", ProjectUUID: uuid.New(), EnvironmentUUID: uuid.New(), FirstForwardDeploymentID: uuid.New(), RollbackBaseDeploymentID: uuid.New()}
	first, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = first.Rollback() }()
	missing, err := LockExternalOperationRollbackAnchor(ctx, first, anchor)
	require.NoError(t, err)
	require.Nil(t, missing)
	second, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = second.Rollback() }()
	var pid int
	require.NoError(t, second.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	type result struct {
		anchor *ExternalOperationRollbackAnchor
		err    error
	}
	done := make(chan result, 1)
	go func() {
		value, err := LockExternalOperationRollbackAnchor(ctx, second, anchor)
		done <- result{value, err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := admin.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')", pid).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, CreateExternalOperationRollbackAnchor(ctx, first, anchor))
	require.NoError(t, first.Commit())
	select {
	case value := <-done:
		require.NoError(t, value.err)
		require.Equal(t, anchor, *value.anchor)
	case <-ctx.Done():
		t.Fatal("second first-admission waiter did not observe immutable anchor")
	}
	require.NoError(t, second.Commit())
	for _, field := range []string{"project UUID", "project slug", "Environment slug"} {
		t.Run(field, func(t *testing.T) {
			wrong := anchor
			switch field {
			case "project UUID":
				wrong.ProjectUUID = uuid.New()
			case "project slug":
				wrong.ProjectID = "other"
			case "Environment slug":
				wrong.EnvID = "other"
			}
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = LockExternalOperationRollbackAnchor(ctx, tx, wrong)
			require.ErrorContains(t, err, "scope changed")
		})
	}
}

func TestRollbackAnchorMigrationAdoptsOnlyUnambiguousRetainedFirstForward(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	for _, test := range []struct {
		name         string
		forwards     int
		unknownScope bool
	}{
		{name: "unambiguous", forwards: 1},
		{name: "ambiguous", forwards: 2},
		{name: "ambiguous nullable legacy Environment", forwards: 2, unknownScope: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			_, _, scoped := migrationTestDatabase(t, ctx, connection)
			legacy, err := sql.Open("postgres", scoped)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, legacy.Close()) })
			source, err := fs.Sub(embedMigrations, "migrations")
			require.NoError(t, err)
			provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source,
				goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2,
					&goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
			require.NoError(t, err)
			_, err = provider.UpTo(ctx, 29)
			require.NoError(t, err)
			env, project, actor, base, first := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			op := uuid.NewString()
			seed, err := legacy.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = seed.Rollback() }()
			_, err = seed.ExecContext(ctx, `INSERT INTO deployment_environments(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at)
				VALUES($1,'org','project','env',$2,now(),$2,now())`, env, base)
			require.NoError(t, err)
			_, err = seed.ExecContext(ctx, `INSERT INTO deployments(de_id,id,created_by,created_at,mode,status,status_message,completed_at,manifest,graph,tofu,runner_id,runner_log_level)
				VALUES($1,$2,$3,now(),'deploy','succeeded','base',now(),'{}','{}',$4,'runner','info')`, env, base, actor, []byte("terraform {}"))
			require.NoError(t, err)
			for i := 0; i < test.forwards; i++ {
				id := first
				if i > 0 {
					id = uuid.New()
				}
				var receiptEnvironment any = env
				if i > 0 && test.unknownScope {
					receiptEnvironment = nil
				}
				_, err = seed.ExecContext(ctx, `INSERT INTO deployments(de_id,id,created_by,created_at,mode,status,status_message,completed_at,manifest,graph,tofu,runner_id,runner_log_level)
					VALUES($1,$2,$3,now(),'deploy','failed','terminal',now(),'{}','{}',$4,'runner','info')`, env, id, actor, []byte("terraform {}"))
				require.NoError(t, err)
				_, err = seed.ExecContext(ctx, `INSERT INTO external_operation_deployments(org_id,plugin_id,operation_ref,fence,request_id,assertion_action,service_action,request_body_sha256,actor_id,project_id,env_id,project_uuid,environment_uuid,base_deployment_id,deployment_id)
					VALUES('org','dev.example.rollouts',$1,1,$2,'external-deployment.admit','external-deployment.admit',$3,$4,'project','env',$5,$6,$7,$8)`,
					op, uuid.NewString(), strings.Repeat("a", 64), actor.String(), project, receiptEnvironment, base, id)
				require.NoError(t, err)
			}
			require.NoError(t, seed.Commit())
			_, err = provider.Up(ctx)
			require.NoError(t, err)
			// Normal startup/restart must preserve the exact adopted anchor.
			for restart := 0; restart < 2; restart++ {
				store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
				require.NoError(t, err)
				tx, err := store.BeginTx(ctx, nil)
				require.NoError(t, err)
				anchor, err := ReadExternalOperationRollbackAnchor(ctx, tx, ExternalOperationRollbackAnchor{OrgID: "org", PluginID: "dev.example.rollouts", OperationRef: op, ProjectID: "project", EnvID: "env", ProjectUUID: project, EnvironmentUUID: env})
				require.NoError(t, err)
				if test.forwards == 1 {
					require.NotNil(t, anchor)
					require.Equal(t, first, anchor.FirstForwardDeploymentID)
					require.Equal(t, base, anchor.RollbackBaseDeploymentID)
				} else {
					require.Nil(t, anchor, "ambiguous history must not pick a first timestamp")
				}
				require.NoError(t, tx.Rollback())
				require.NoError(t, store.Close())
			}
			if test.forwards == 1 {
				// A retained receipt remains downgrade evidence even after its
				// ordinary Deployment has been explicitly deleted with the Env.
				_, err = legacy.ExecContext(ctx, `INSERT INTO external_operation_deployments
					(org_id,plugin_id,operation_ref,fence,request_id,assertion_action,service_action,request_body_sha256,actor_id,project_id,env_id,project_uuid,environment_uuid,base_deployment_id,deployment_id,forward_deployment_id,retry_of_rollback_deployment_id)
					VALUES('org','dev.example.rollouts',$1,1,$2,'external-deployment.admit','external-deployment.rollback',$3,$4,'project','env',$5,$6,$7,$8,$9,$10)`,
					op, uuid.NewString(), strings.Repeat("b", 64), actor.String(), project, env, base, uuid.New(), first, uuid.New())
				require.NoError(t, err)
				_, err = provider.DownTo(ctx, 29)
				require.ErrorContains(t, err, "cannot remove explicit rollback retry predecessor evidence")
				var anchors int
				require.NoError(t, legacy.QueryRowContext(ctx, "SELECT count(*) FROM external_operation_rollback_anchors").Scan(&anchors))
				require.Equal(t, 1, anchors)
			}
		})
	}
}

func TestRollbackAnchorFreshAndVersion29UpgradeHaveExactSchemaContract(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, freshSchema, freshConnection := migrationTestDatabase(t, ctx, connection)
	fresh, err := NewDatabaser(ctx, zap.NewNop(), freshConnection)
	require.NoError(t, err)
	defer func() { require.NoError(t, fresh.Close()) }()
	_, upgradeSchema, upgradeConnection := migrationTestDatabase(t, ctx, connection)
	legacy, err := sql.Open("postgres", upgradeConnection)
	require.NoError(t, err)
	defer func() { require.NoError(t, legacy.Close()) }()
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source, goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.UpTo(ctx, 29)
	require.NoError(t, err)
	upgraded, err := NewDatabaser(ctx, zap.NewNop(), upgradeConnection)
	require.NoError(t, err)
	defer func() { require.NoError(t, upgraded.Close()) }()
	contract := func(schema string) []string {
		rows, err := admin.QueryContext(ctx, `
			SELECT definition FROM (
			 SELECT 'column:'||c.relname||':'||a.attnum||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||coalesce(pg_get_expr(d.adbin,d.adrelid),'') AS definition
			 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid
			 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
			 WHERE n.nspname=$1 AND c.relkind='r' AND c.relname<>'goose_db_version' AND a.attnum>0 AND NOT a.attisdropped
			 UNION ALL
			 SELECT 'constraint:'||c.relname||':'||k.conname||':'||pg_get_constraintdef(k.oid)
			 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
			 WHERE n.nspname=$1 AND c.relname<>'goose_db_version'
			 UNION ALL
			 SELECT 'index:'||c.relname||':'||pg_get_indexdef(i.indexrelid)
			 FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
			 WHERE n.nspname=$1 AND c.relname<>'goose_db_version'
			) definitions ORDER BY definition`, schema)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		var definitions []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			definitions = append(definitions, strings.ReplaceAll(value, schema+".", ""))
		}
		require.NoError(t, rows.Err())
		return definitions
	}
	require.Equal(t, contract(freshSchema), contract(upgradeSchema))
}

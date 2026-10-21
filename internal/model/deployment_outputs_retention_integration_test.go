//go:build integration

package model

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestDeploymentOutputsRetentionRequiresExactExternalMaterialOwnership(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	for _, test := range []struct {
		name string
		edit string
		keep bool
	}{
		{name: "owned deploy", keep: true},
		{name: "owned rollback", keep: true},
		{name: "ordinary transient", edit: "DELETE FROM external_operation_deployments WHERE deployment_id=$1"},
		{name: "missing material", edit: "DELETE FROM external_operation_material"},
		{name: "foreign receipt organization", edit: "UPDATE external_operation_deployments SET org_id='other' WHERE deployment_id=$1"},
		{name: "foreign material organization", edit: "UPDATE external_operation_material SET org_id='other'"},
		{name: "foreign material plugin", edit: "UPDATE external_operation_material SET plugin_id='dev.example.other'"},
		{name: "foreign material operation", edit: "UPDATE external_operation_material SET operation_ref='11111111-1111-4111-8111-111111111111'"},
		{name: "foreign recipient", edit: "UPDATE external_operation_material SET recipient='another'"},
		{name: "missing wrapped identity", edit: "UPDATE external_operation_material SET encrypted_identity=''"},
		{name: "unwrapped identity", edit: "UPDATE external_operation_material SET encrypted_identity='not-wrapped'"},
		{name: "missing deployment recipient", edit: "UPDATE deployments SET encrypted_outputs_recipient=NULL WHERE id=$1"},
		{name: "missing project UUID", edit: "UPDATE external_operation_deployments SET project_uuid=NULL WHERE deployment_id=$1"},
		{name: "foreign Environment UUID", edit: "UPDATE external_operation_deployments SET environment_uuid='11111111-1111-4111-8111-111111111111' WHERE deployment_id=$1"},
		{name: "foreign project slug", edit: "UPDATE external_operation_deployments SET project_id='other' WHERE deployment_id=$1"},
		{name: "foreign Environment slug", edit: "UPDATE external_operation_deployments SET env_id='other' WHERE deployment_id=$1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			const orgID, projectID, envID, pluginID = "retention-org", "project", "environment", "dev.example.retention"
			identity, err := age.GenerateX25519Identity()
			require.NoError(t, err)
			operationID, environmentID := uuid.New(), uuid.New()
			mode := DeploymentModeDeploy
			if test.name == "owned rollback" {
				mode = DeploymentModeRollback
			}
			deployment, err := store.CreateDeployment(ctx, tx, orgID, projectID, envID, CreateDeploymentParams{
				CreatedBy: uuid.New(), DeploymentEnvUuid: environmentID, Mode: mode,
				Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
				RunnerId: "runner", RunnerLogLevel: "info", EncryptedOutputsRecipient: opt.Of(identity.Recipient().String()),
			})
			require.NoError(t, err)
			_, err = StoreExternalOperationMaterial(ctx, tx, ExternalOperationMaterial{
				OrgID: orgID, PluginID: pluginID, OperationRef: operationID, Recipient: identity.Recipient().String(), EncryptedIdentity: "vault:v1:wrapped",
			})
			require.NoError(t, err)
			_, err = CreateExternalOperationDeploymentReceipt(ctx, tx, ExternalOperationDeploymentReceipt{
				ExternalOperationDeploymentKey: ExternalOperationDeploymentKey{OrgID: orgID, PluginID: pluginID, RequestID: uuid.NewString()},
				OperationRef:                   operationID.String(), Fence: 7, AssertionAction: "external-deployment.admit", ServiceAction: "external-deployment.admit",
				RequestBodySHA256: strings.Repeat("a", 64), ActorID: deployment.CreatedBy.String(), ProjectID: projectID, EnvID: envID,
				ProjectUUID: opt.Of(uuid.New()), EnvironmentUUID: opt.Of(environmentID), DeploymentID: deployment.Id,
			})
			require.NoError(t, err)
			const ciphertext = "original-encrypted-output"
			_, err = store.UpdateDeploymentStatusAndOutputs(ctx, tx, deployment.Id, UpdateDeploymentStatusAndOutputsParams{
				Status: DeploymentStatusSucceeded, Outputs: opt.Of(ciphertext),
			})
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "UPDATE deployments SET completed_at=now()-interval '10 minutes' WHERE id=$1", deployment.Id)
			require.NoError(t, err)
			if test.edit != "" {
				if strings.Contains(test.edit, "$1") {
					_, err = tx.ExecContext(ctx, test.edit, deployment.Id)
				} else {
					_, err = tx.ExecContext(ctx, test.edit)
				}
				require.NoError(t, err)
			}
			cleaned, err := store.DeleteDeploymentOutputsByCompletionDate(ctx, tx, time.Now().Add(-5*time.Minute))
			require.NoError(t, err)
			actual, err := store.GetDeploymentEncryptedOutputs(ctx, tx, orgID, deployment.Id)
			require.NoError(t, err)
			if test.keep {
				require.Empty(t, cleaned)
				require.Equal(t, ciphertext, actual)
				// Retention ends at actual normal Deployment deletion, not grant
				// release or a resource's archival/visibility state in Core.
				require.NoError(t, store.DeleteDeploymentsForEnv(ctx, tx, orgID, projectID, envID, false))
				_, err = store.GetDeploymentEncryptedOutputs(ctx, tx, orgID, deployment.Id)
				_, missing := IsErrNotFound(err)
				require.True(t, missing)
				var receipts int
				require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM external_operation_deployments WHERE deployment_id=$1", deployment.Id).Scan(&receipts))
				require.Equal(t, 1, receipts, "replay tombstone outlives material retention")
			} else {
				require.Len(t, cleaned, 1)
				require.Equal(t, deployment.Id, cleaned[0].Id)
				require.Empty(t, actual)
			}
		})
	}
}

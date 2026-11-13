//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"filippo.io/age"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

func managedLogVault(t *testing.T) vault.VaultClientInterface {
	t.Helper()
	address := os.Getenv("COST_POLICY_TEST_VAULT_ADDR")
	require.NotEmpty(t, address, "requires an owned real Vault Transit fixture")
	config := vaultapi.DefaultConfig()
	config.Address = address
	client, err := vaultapi.NewClient(config)
	require.NoError(t, err)
	client.SetToken(os.Getenv("COST_POLICY_TEST_VAULT_TOKEN"))
	return vault.NewVaultClient(client, zap.NewNop())
}

func TestManagedDeploymentLogsCreationAndRetrieval(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.Vault = managedLogVault(t)
	var encrypted []byte
	f.server.RunnerLogsReader = func(context.Context, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encrypted)), nil
	}
	first := f.mustDeploy(queueBody(map[string]any{"managed_logs": true}))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	material, err := model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, first.Id)
	require.NoError(t, err)
	require.Contains(t, material.EncryptedIdentity, "vault:v")
	require.NotContains(t, material.EncryptedIdentity, "AGE-SECRET-KEY")
	require.NoError(t, tx.Commit())
	identity, err := (DeploymentLogMaterialService{Vault: f.server.Vault}).Identity(f.ctx, *material)
	require.NoError(t, err)
	var ciphertext bytes.Buffer
	recipient, err := age.ParseX25519Recipient(material.Recipient)
	require.NoError(t, err)
	writer, err := age.Encrypt(&ciphertext, recipient)
	require.NoError(t, err)
	_, err = writer.Write([]byte("runner private output\n"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	encrypted = []byte(base64.StdEncoding.EncodeToString(ciphertext.Bytes()))
	path := "/orgs/" + queueOrg + "/deployments/" + first.Id.String() + "/actions/get-logs"
	raw := f.call(http.MethodGet, path, nil, f.actor, "")
	require.Equal(t, http.StatusOK, raw.Code, raw.Body.String())
	require.Equal(t, ciphertext.String(), raw.Body.String(), "legacy no-header response stays encrypted")
	plain := f.call(http.MethodGet, path+"?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusOK, plain.Code, plain.Body.String())
	require.Equal(t, "runner private output\n", plain.Body.String())
	require.Equal(t, "no-store", plain.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", plain.Header().Get("X-Content-Type-Options"))
	manual := f.request(http.MethodGet, path, nil, f.actor, "")
	manual.Header.Set("DecryptKey", identity.(*age.X25519Identity).String())
	require.Equal(t, http.StatusOK, f.serve(manual).Code)
	ambiguous := f.request(http.MethodGet, path+"?decrypt=true", nil, f.actor, "")
	ambiguous.Header.Set("DecryptKey", identity.(*age.X25519Identity).String())
	require.Equal(t, http.StatusBadRequest, f.serve(ambiguous).Code)
	savedVault := f.server.Vault
	outageConfig := vaultapi.DefaultConfig()
	outageConfig.Address, outageConfig.Timeout, outageConfig.MaxRetries = "http://127.0.0.1:1", time.Second, 0
	outageClient, err := vaultapi.NewClient(outageConfig)
	require.NoError(t, err)
	outageClient.SetToken("owned-fixture-only")
	f.server.Vault = vault.NewVaultClient(outageClient, zap.NewNop())
	unavailable := f.call(http.MethodGet, path+"?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusInternalServerError, unavailable.Code)
	require.NotContains(t, unavailable.Body.String(), "runner private output")
	f.server.Vault = savedVault
	savedEncrypted := encrypted
	encrypted = bytes.Repeat([]byte("a"), maxExternalMaterialEncodedBytes+1)
	oversized := f.call(http.MethodGet, path+"?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusInternalServerError, oversized.Code)
	require.Less(t, oversized.Body.Len(), 1024)
	encrypted = savedEncrypted
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, "UPDATE deployment_log_material SET recipient='wrong' WHERE deployment_id=$1", first.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	mismatch := f.call(http.MethodGet, path+"?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusConflict, mismatch.Code, mismatch.Body.String())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, "UPDATE deployment_log_material SET recipient=$2 WHERE deployment_id=$1", first.Id, material.Recipient)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	missingResponse := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+uuid.NewString()+"/actions/get-logs?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusNotFound, missingResponse.Code, missingResponse.Body.String())
	wrongOrg := f.call(http.MethodGet, "/orgs/other-org/deployments/"+first.Id.String()+"/actions/get-logs?decrypt=true", nil, f.actor, "")
	require.Equal(t, http.StatusNotFound, wrongOrg.Code, wrongOrg.Body.String())
	f.iam.deny(f.actor, true)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodGet, path+"?decrypt=true", nil, f.actor, "").Code)
	require.Equal(t, http.StatusForbidden, f.serve(manual).Code)
	f.iam.deny(f.actor, false)
	queued := f.mustQueue(f.deploy(queueBody(map[string]any{"managed_logs": true, "when_busy": "queue"})))
	f.finish(first.Id, model.DeploymentStatusSucceeded)
	next := f.item(queued.Id).DeploymentID.Must()
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	nextMaterial, err := model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, next)
	require.NoError(t, err)
	require.NotEqual(t, material.Recipient, nextMaterial.Recipient, "queue promotion creates its own deployment identity")
	_, err = model.GetDeploymentLogMaterial(f.ctx, tx, "other-org", next)
	_, missing := model.IsErrNotFound(err)
	require.True(t, missing)
	require.NoError(t, tx.Commit())
}

func TestManagedDeploymentLogsPreserveExplicitAndSkippedIntent(t *testing.T) {
	for _, mode := range []string{"skipped", "explicit", "dry-run"} {
		t.Run(mode, func(t *testing.T) {
			f := newQueueFixture(t, DeploymentQueueConfig{})
			fields := map[string]any{}
			if mode == "explicit" {
				identity, err := age.GenerateX25519Identity()
				require.NoError(t, err)
				fields["encrypted_logs_recipient"] = identity.Recipient().String()
			}
			if mode == "dry-run" {
				fields["managed_logs"] = true
				fields["is_dry_run"] = true
			}
			// No Vault is configured: these paths must not try to create a key.
			var deployment Deployment
			if mode == "dry-run" {
				response := f.deploy(queueBody(fields))
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				deployment.Id = uuid.Nil
			} else {
				deployment = f.mustDeploy(queueBody(fields))
			}
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			_, err = model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, deployment.Id)
			_, missing := model.IsErrNotFound(err)
			require.True(t, missing)
			require.NoError(t, tx.Commit())
			if mode != "dry-run" {
				response := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+deployment.Id.String()+"/actions/get-logs?decrypt=true", nil, f.actor, "")
				require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			}
		})
	}
}

func TestManagedDeploymentLogsBindIdempotencyAndRollback(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.Vault = managedLogVault(t)
	path := "/orgs/" + queueOrg + "/deployments"
	body := queueBody(map[string]any{"managed_logs": true})
	response := f.call(http.MethodPost, path, body, f.actor, "managed-replay")
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	var first Deployment
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &first))
	replay := f.call(http.MethodPost, path, body, f.actor, "managed-replay")
	require.Equal(t, http.StatusCreated, replay.Code, replay.Body.String())
	var repeated Deployment
	require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &repeated))
	require.Equal(t, first.Id, repeated.Id)
	changed := f.call(http.MethodPost, path, queueBody(nil), f.actor, "managed-replay")
	require.Equal(t, http.StatusConflict, changed.Code, changed.Body.String())
	callerIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	conflict := f.deploy(queueBody(map[string]any{"managed_logs": true, "encrypted_logs_recipient": callerIdentity.Recipient().String()}))
	require.Equal(t, http.StatusBadRequest, conflict.Code, conflict.Body.String())
	f.finish(first.Id, model.DeploymentStatusSucceeded)
	rollback := f.mustDeploy(queueBody(map[string]any{"managed_logs": true, "manifest": nil, "mode": "rollback", "rollback_to_deployment_id": first.Id}))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	material, err := model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, rollback.Id)
	require.NoError(t, err)
	original, err := model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, first.Id)
	require.NoError(t, err)
	require.NotEqual(t, original.Recipient, material.Recipient)
	require.NoError(t, tx.Commit())
}

//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	iam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/require"
)

// Real PG/Vault/NATS custody and API authorization. The target/command are
// persisted fixture authority; this does not prove production Core admission.
func TestNativePhaseLogsManagedReceiptAndRead(t *testing.T) {
	f := newNoEffectFixture(t)
	target := nativeTargetFixture()
	target.RunnerId = "site"
	dep := createNativeDispatchDeployment(t, f, target, "native-logs")
	s := f.server
	s.Vault = managedLogVault(t)
	material, err := (DeploymentLogMaterialService{Vault: s.Vault}).Generate(f.ctx)
	require.NoError(t, err)
	dep.EncryptedLogsRecipient = opt.Of(material.Recipient)
	tx, err := s.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET encrypted_logs_recipient=$2 WHERE id=$1`, dep.Id, material.Recipient)
	require.NoError(t, err)
	require.NoError(t, model.StoreDeploymentLogMaterial(f.ctx, tx, dep.OrgId, dep.Id, *material))
	require.NoError(t, tx.Commit())
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	definition, err := model.ResolvePipeline(nil, dep.Mode)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: s, Key: key}
	frozen, err := dispatcher.signCommand(dep, *definition, []byte("bounded accepted command"), json.RawMessage(`{}`), string(target.StateBackendType), nil, nil, material.Recipient)
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(frozen, dep)
	require.NoError(t, err)
	tx, err = s.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, frozen)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	address := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, address, "requires an owned actual NATS fixture")
	connection, err := nats.Connect(address, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	require.NoError(t, err)
	objects, err := js.ObjectStore(f.ctx, nativelogs.Bucket)
	if err != nil {
		objects, err = js.CreateObjectStore(f.ctx, jetstream.ObjectStoreConfig{Bucket: nativelogs.Bucket, Storage: jetstream.MemoryStorage})
	}
	require.NoError(t, err)
	var reads atomic.Int64
	s.RunnerLogsReader = func(ctx context.Context, key string) (io.ReadCloser, error) {
		reads.Add(1)
		return objects.Get(ctx, key)
	}
	recipient, err := age.ParseX25519Recipient(material.Recipient)
	require.NoError(t, err)
	a := nativelogs.Authority{
		OrganizationID: dep.OrgId,
		EnvironmentID:  dep.DeploymentEnvUuid.String(),
		DeploymentID:   dep.Id.String(),
		CommandDigest:  nativelogs.Hash(payload),
		Target: nativelogs.Target{
			RunnerID:                 target.RunnerId,
			RunnerUUID:               target.RunnerUuid.String(),
			RuntimeVersion:           string(target.RuntimeVersion),
			PipelineVersion:          string(target.PipelineVersion),
			ToolsDigest:              target.ToolsDigest,
			StateBackendType:         string(target.StateBackendType),
			StateConfigurationDigest: target.StateConfigurationDigest,
		},
		Phase:           "apply",
		ExecutionID:     dep.Id.String(),
		CaptureRevision: 2,
	}
	ciphertext, err := nativelogs.Encode(nativelogs.Envelope{Version: 1, Authority: a, Status: nativelogs.PendingTransfer, Logs: []byte("actual private log\n")}, recipient)
	require.NoError(t, err)
	d := nativelogs.Descriptor{Version: 1, Authority: a, Status: nativelogs.PendingTransfer, CiphertextSHA256: nativelogs.Hash(ciphertext), CiphertextBytes: len(ciphertext), ObjectKey: a.ObjectKey(nativelogs.Hash(ciphertext))}
	_, err = objects.Put(f.ctx, jetstream.ObjectMeta{Name: d.ObjectKey}, bytes.NewReader(ciphertext))
	require.NoError(t, err)
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	descriptorJSON, err := json.Marshal(d)
	require.NoError(t, err)
	event := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: dep.OrgId, RunnerID: route, DeploymentID: dep.Id.String(), EventID: d.EventID(), Type: nativelogs.ReadyEventType, CreatedAt: time.Now().UTC(), Payload: descriptorJSON}
	require.NoError(t, s.ApplyNativeLogReady(f.ctx, event))
	require.NoError(t, s.ApplyNativeLogReady(f.ctx, event))
	actor := uuid.New()
	ctx := context.WithValue(f.ctx, hecho.ContextKeyUserID, actor.String())
	for _, granted := range [][]string{{PermissionDeploymentRead}, {PermissionDeploymentLogsRead}, {}, {PermissionDeploymentRead, PermissionDeploymentLogsRead}} {
		allowed := map[string]bool{}
		for _, permission := range granted {
			allowed[permission] = true
		}
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body iam.InternalAuthorizeBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode authorization request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !allowed[body.Checks[0].Permission] {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"HTTP-403","message":"denied","details":{}}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		client, err := iam.NewClientWithResponses(peer.URL)
		require.NoError(t, err)
		s.IamClient = client
		before := reads.Load()
		response, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{OrgId: dep.OrgId, DeploymentId: dep.Id, Params: GetDeploymentLogsParams{Decrypt: ref.Ref(true)}})
		if len(granted) == 2 {
			require.NoError(t, err)
			require.Equal(t, "actual private log\n", response.(GetDeploymentLogs200TextResponse).Body)
			require.Greater(t, reads.Load(), before)
		} else {
			require.Error(t, err)
			require.Equal(t, before, reads.Load(), "both roles required before any object read")
		}
		peer.Close()
	}
	bad := event
	wrong := d
	wrong.Authority.Target.ToolsDigest = strings.Repeat("f", 64)
	bad.Payload, err = json.Marshal(wrong)
	require.NoError(t, err)
	bad.EventID = wrong.EventID()
	before := reads.Load()
	require.ErrorIs(t, s.ApplyNativeLogReady(f.ctx, bad), ErrRunnerEventTargetInvalid)
	require.Equal(t, before, reads.Load())
	tx, err = s.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	run, err := model.ReadDeploymentPipeline(f.ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Len(t, run.Progress.LogReferences, 1)
	require.Equal(t, "accepted", run.Progress.Phase)
	raw, err := json.Marshal(run)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "actual private log")
	require.NotContains(t, string(raw), "AGE-SECRET-KEY")
	require.NoError(t, tx.Rollback())
	require.NoError(t, objects.Delete(f.ctx, d.ObjectKey))
	tx, err = s.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = s.loadNativeLog(f.ctx, tx, dep, d)
	require.Error(t, err, "retention loss is availability, never replanning or identity generation")
}

func TestNativePhaseLogSourceChildFrozenAuthority(t *testing.T) {
	f := newNoEffectFixture(t)
	target := nativeTargetFixture()
	target.RunnerId = "site"
	dep := createNativeDispatchDeployment(t, f, target, "child-logs")
	recipient, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	dep.EncryptedLogsRecipient = opt.Of(recipient.Recipient().String())
	sourceID, sourceSubject, childSubject := uuid.NewString(), strings.Repeat("a", 64), strings.Repeat("b", 64)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	definition, err := model.ResolvePipeline(nil, dep.Mode)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: key}
	raw, err := dispatcher.signCommand(dep, *definition, []byte("child accepted closure"), json.RawMessage(`{}`), string(target.StateBackendType), nil, &pipelineSourcePlan{SourcePlanID: sourceID, Subject: sourceSubject, BaseDeploymentID: uuid.NewString()}, dep.EncryptedLogsRecipient.Must())
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(raw, dep)
	require.NoError(t, err)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET encrypted_logs_recipient=$2 WHERE id=$1`, dep.Id, dep.EncryptedLogsRecipient.Must())
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, raw)
	require.NoError(t, err)
	require.NoError(t, model.UpdatePipelineProgress(f.ctx, tx, model.PipelineProgress{ID: dep.Id.String(), Subject: childSubject, Revision: 5, Phase: "planned"}))
	a := nativelogs.Authority{
		OrganizationID: dep.OrgId,
		EnvironmentID:  dep.DeploymentEnvUuid.String(),
		DeploymentID:   dep.Id.String(),
		CommandDigest:  nativelogs.Hash(payload),
		Target: nativelogs.Target{
			RunnerID:                 target.RunnerId,
			RunnerUUID:               target.RunnerUuid.String(),
			RuntimeVersion:           string(target.RuntimeVersion),
			PipelineVersion:          string(target.PipelineVersion),
			ToolsDigest:              target.ToolsDigest,
			StateBackendType:         string(target.StateBackendType),
			StateConfigurationDigest: target.StateConfigurationDigest,
		},
		Phase:           "plan",
		ExecutionID:     sourceID,
		CaptureRevision: 3,
		Subject:         childSubject,
		SourcePlanID:    sourceID,
		SourceSubject:   sourceSubject,
		Inherited:       true,
	}
	descriptor := nativelogs.Descriptor{Version: 1, Authority: a, Status: nativelogs.PendingTransfer, CiphertextSHA256: strings.Repeat("d", 64), CiphertextBytes: 100, ObjectKey: a.ObjectKey(strings.Repeat("d", 64))}
	_, err = nativeLogCommand(f.ctx, tx, dep, descriptor, nil)
	require.NoError(t, err)
	for name, alter := range map[string]func(*nativelogs.Authority){
		"fake new plan execution":   func(a *nativelogs.Authority) { a.ExecutionID = dep.Id.String() },
		"lost inherited provenance": func(a *nativelogs.Authority) { a.Inherited = false },
		"foreign source":            func(a *nativelogs.Authority) { a.SourcePlanID = uuid.NewString() },
		"source subject":            func(a *nativelogs.Authority) { a.SourceSubject = strings.Repeat("f", 64) },
		"child subject":             func(a *nativelogs.Authority) { a.Subject = sourceSubject },
		"command digest":            func(a *nativelogs.Authority) { a.CommandDigest = strings.Repeat("e", 64) },
		"runner slug":               func(a *nativelogs.Authority) { a.Target.RunnerID = "another" },
		"runner UUID":               func(a *nativelogs.Authority) { a.Target.RunnerUUID = uuid.NewString() },
		"runtime":                   func(a *nativelogs.Authority) { a.Target.RuntimeVersion = "v3" },
		"pipeline":                  func(a *nativelogs.Authority) { a.Target.PipelineVersion = "v3" },
		"tools":                     func(a *nativelogs.Authority) { a.Target.ToolsDigest = strings.Repeat("e", 64) },
		"backend":                   func(a *nativelogs.Authority) { a.Target.StateBackendType = "another" },
		"state digest":              func(a *nativelogs.Authority) { a.Target.StateConfigurationDigest = strings.Repeat("e", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := descriptor
			alter(&bad.Authority)
			bad.ObjectKey = bad.Authority.ObjectKey(bad.CiphertextSHA256)
			_, err := nativeLogCommand(f.ctx, tx, dep, bad, nil)
			require.Error(t, err)
		})
	}
}

func TestNativePhaseLogDispatchUsesExistingManagedCustodyOnly(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller-only", true: "managed"}[managed], func(t *testing.T) {
			f := newNativeDispatchFixture(t)
			var material *model.DeploymentLogMaterial
			var err error
			if managed {
				f.server.Vault = managedLogVault(t)
				material, err = (DeploymentLogMaterialService{Vault: f.server.Vault}).Generate(f.ctx)
				require.NoError(t, err)
			} else {
				identity, err := age.GenerateX25519Identity()
				require.NoError(t, err)
				material = &model.DeploymentLogMaterial{Recipient: identity.Recipient().String()}
			}
			f.dep.EncryptedLogsRecipient = opt.Of(material.Recipient)
			tx, err := f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET encrypted_logs_recipient=$2 WHERE id=$1`, f.dep.Id, material.Recipient)
			require.NoError(t, err)
			if managed {
				require.NoError(t, model.StoreDeploymentLogMaterial(f.ctx, tx, f.dep.OrgId, f.dep.Id, *material))
			}
			require.NoError(t, tx.Commit())
			require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep))
			message := f.last()
			payload, err := model.FrozenPipelinePayload(message.Data, f.dep)
			require.NoError(t, err)
			var command pipelineCommand
			require.NoError(t, json.Unmarshal(payload, &command))
			if managed {
				require.Equal(t, material.Recipient, command.Request.EncryptedLogsRecipient)
			} else {
				require.Empty(t, command.Request.EncryptedLogsRecipient)
				require.NotContains(t, string(payload), `"encrypted_logs_recipient"`)
			}
			tx, err = f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			retained, err := model.GetDeploymentLogMaterial(f.ctx, tx, f.dep.OrgId, f.dep.Id)
			if managed {
				require.NoError(t, err)
				require.Equal(t, material, retained)
			} else {
				_, missing := model.IsErrNotFound(err)
				require.True(t, missing)
			}
			require.NoError(t, tx.Rollback())
			before := append([]byte{}, message.Data...)
			f.server.Vault = nil
			require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep))
			require.Equal(t, before, f.last().Data, "frozen replay never mints or unwraps a new key")
			tx, err = f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = tx.ExecContext(f.ctx, `ALTER TABLE deployment_log_material RENAME TO log_material_unavailable`)
			require.NoError(t, err)
			definition, resolveErr := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
			require.NoError(t, resolveErr)
			_, err = f.dispatcher.buildCommand(f.ctx, tx, f.dep, *definition, "kubernetes")
			require.Error(t, err, "a material query failure is not caller-only absence")
		})
	}
}

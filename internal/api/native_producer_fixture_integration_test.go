//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The Runner qualification invokes this peer in a separate test executable.
// Files are a private fixture handoff, not a product command/activation API.
func TestNativeProducerFixturePeer(t *testing.T) {
	path := os.Getenv("D2_NATIVE_PRODUCER_FIXTURE")
	if path == "" {
		t.Skip("selected by the explicit cross-component qualification gate")
	}
	fixture, err := os.OpenRoot(filepath.Dir(path))
	require.NoError(t, err)
	defer func() { require.NoError(t, fixture.Close()) }()
	raw, err := fixture.ReadFile(filepath.Base(path))
	require.NoError(t, err)
	var input struct{ Org, Runner, RunnerUUID, Environment, Namespace, ToolsDigest, Recipient, SigningKey, Directory string }
	require.NoError(t, json.Unmarshal(raw, &input))
	key, err := base64.StdEncoding.DecodeString(input.SigningKey)
	require.NoError(t, err)
	if len(key) != ed25519.PrivateKeySize {
		t.Fatal("fixture signing key has invalid length")
	}
	f := newNoEffectFixture(t)
	storage := json.RawMessage(fmt.Sprintf(`{"type":"kubernetes","namespace":%q}`, input.Namespace))
	backend, digest, err := cp.StateStorageConfigurationDigest(storage)
	require.NoError(t, err)
	target := &cp.NativeExecutionTargetReference{RunnerId: input.Runner, RunnerUuid: uuid.MustParse(input.RunnerUUID), RuntimeVersion: cp.NativeTargetRuntimeV2, PipelineVersion: cp.NativeTargetPipelineV2, ToolsDigest: input.ToolsDigest, StateBackendType: backend, StateConfigurationDigest: digest}
	brokerURL := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, brokerURL, "the coupled fixture requires its owned application broker")
	require.Equal(t, brokerURL, os.Getenv("AGENT_DELIVERY_NATS_URL"), "DP and Agent must use the same actual broker")
	connection, err := nats.Connect(brokerURL, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	modern, err := jetstream.New(connection)
	require.NoError(t, err)
	broker, err := connection.JetStream()
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: key, Publisher: hnats.NewPublisher(modern, hmessaging.RunnerCommandsStreamName, zap.NewNop())}
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	current := qualifiedNativeRunner(t)
	current.OrgId, current.Id, current.Uuid = input.Org, input.Runner, &target.RunnerUuid
	require.NoError(t, json.Unmarshal(storage, &current.StateStorageConfiguration))
	config, err := current.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	protocols := append(*config.SupportedProtocols, "environment-observation/v1")
	config.SupportedProtocols, config.NativeExecution.ToolsDigest = &protocols, input.ToolsDigest
	capabilities := append(*config.Capabilities, "environment-observation/v1")
	config.Capabilities = &capabilities
	require.NoError(t, current.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	project, eligible := uuid.New(), true
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body any
		switch r.URL.Path {
		case "/orgs/" + input.Org + "/projects/project/envs/environment":
			body = cp.Environment{Id: "environment", ProjectId: "project", ProjectUuid: &project, Uuid: uuid.MustParse(input.Environment),
				RunnerId: &input.Runner, NativeFirstEligible: &eligible, Status: cp.EnvironmentStatusActive}
		case "/internal/orgs/" + input.Org + "/runners/" + input.Runner:
			body = cp.InternalRunner{OrgId: current.OrgId, Id: current.Id, Uuid: current.Uuid,
				RunnerConfiguration: current.RunnerConfiguration, StateStorageConfiguration: current.StateStorageConfiguration,
				RunnerConfigurationSecret: cp.ConfigurationSecret{Path: "PRIVATE_COUPLED_OBSERVATION_REFERENCE", Version: 1}}
		default:
			t.Errorf("unexpected observation metadata request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(body))
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: ed25519.PrivateKey(key).Public().(ed25519.PublicKey),
		Admission: ExternalDeploymentAdmissionService{Database: f.server.Database}, ControlPlaneClient: client, EventPublisher: f.server.EventPublisher}
	router := echo.New()
	router.POST(ObservationStartPath, adapter.HandleObservationStart)
	var successful uuid.UUID
	var provenance json.RawMessage
	var baselineFingerprint string
	for index := range 3 {
		main := fmt.Sprintf(
			"terraform {\n backend \"kubernetes\" {\n namespace = %q\n secret_suffix = %q\n }\n}\nresource \"terraform_data\" \"marker\" { input = \"approved\" }\noutput \"value\" { value = terraform_data.marker.output }\noutput \"platform_orchestrator_metadata\" { value = {} }\n",
			input.Namespace,
			input.Environment,
		)
		if index == 1 {
			main += "invalid syntax (\n"
		}
		actor := uuid.New()
		var dep *model.DeploymentSummary
		if index == 2 {
			now := time.Now().UTC()
			request := observation.ObservationRequest{RequestId: uuid.New(), GrantId: uuid.New(), GrantRevision: 1,
				ProjectId: "project", EnvId: "environment", ProjectUuid: project, EnvironmentUuid: uuid.MustParse(input.Environment),
				Occurrence: now, AdmissionDeadline: now.Add(time.Minute), ExpectedBaselineId: &successful, ExpectedBaselineFingerprint: &baselineFingerprint}
			requestRaw, err := json.Marshal(request)
			require.NoError(t, err)
			path := "/orgs/" + input.Org + "/plugins/dev.stellwerk.fixture/observations/v1/runs"
			assertion, err := plugincontext.Sign(key, plugincontext.Claims{Version: 1, Issuer: "core", Audience: ExternalDeploymentAudience,
				PluginID: "dev.stellwerk.fixture", OrgID: input.Org, ActorID: actor.String(), OperationID: ObservationStartAction,
				Method: http.MethodPost, Path: path, BodySHA256: plugincontext.Digest(requestRaw), RequestID: request.RequestId.String(),
				IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix()}, now)
			require.NoError(t, err)
			httpRequest := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(requestRaw)).WithContext(f.ctx)
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Idempotency-Key", request.RequestId.String())
			httpRequest.Header.Set(plugincontext.Header, assertion)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httpRequest)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "PRIVATE_COUPLED_OBSERVATION_REFERENCE")
			var receipt observation.ObservationReceipt
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
			require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
			require.Equal(t, &successful, receipt.BaselineId)
			require.Equal(t, &baselineFingerprint, receipt.BaselineFingerprint)
			require.NotNil(t, receipt.RunId)
			dep, _, _, _, err = f.server.Database.GetDeployment(f.ctx, nil, input.Org, *receipt.RunId, model.GetModeDefault)
			require.NoError(t, err)
			require.False(t, dep.HasPipeline)
		} else {
			tx, err := f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			dep, err = f.server.Database.CreateDeployment(
				f.ctx,
				tx,
				input.Org,
				"project",
				"environment",
				model.CreateDeploymentParams{
					NativeTarget:              target,
					DeploymentEnvUuid:         uuid.MustParse(input.Environment),
					CreatedBy:                 actor,
					Mode:                      model.DeploymentModeDeploy,
					RunnerId:                  input.Runner,
					RunnerLogLevel:            "info",
					Manifest:                  model.EncodedDeploymentManifest(`{"workloads":{}}`),
					Graph:                     model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
					Tofu:                      model.RawTofu(main),
					Pipeline:                  definition,
					PipelineBackend:           "kubernetes",
					EncryptedOutputsRecipient: opt.Of(input.Recipient),
				},
			)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.NoError(t, f.server.Database.ReserveRunnerDispatch(f.ctx, dep, 2))
		}
		if index == 2 {
			require.NoError(t, dispatcher.DispatchObservation(f.ctx, dep))
		} else {
			require.NoError(t, dispatcher.Dispatch(f.ctx, dep))
		}
		route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
		require.NoError(t, err)
		subject, err := hmessaging.RunnerCommandSubject(input.Org, route)
		require.NoError(t, err)
		published, err := broker.GetLastMsg(hmessaging.RunnerCommandsStreamName, subject)
		require.NoError(t, err)
		messageID, err := hmessaging.NativeMessageID(input.Org, route, "commands", "execute-rollout-v1", "pipeline:"+dep.Id.String())
		require.NoError(t, err)
		require.Equal(t, messageID, published.Header.Get(nats.MsgIdHdr))
		envelope := published.Data
		writeNativeProducerFixture(t, filepath.Join(input.Directory, fmt.Sprintf("command-%d.json", index)), envelope)
		file := fmt.Sprintf("events-%d.json", index)
		var events []hmessaging.Message
		require.Eventually(t, func() bool {
			data, err := fixture.ReadFile(file)
			return err == nil && json.Unmarshal(data, &events) == nil
		}, 35*time.Second, 25*time.Millisecond)
		var result DeploymentResultsUpdateBody
		for _, message := range events {
			var event hmessaging.EventEnvelope
			require.NoError(t, json.Unmarshal(message.Data, &event))
			expected, err := hmessaging.NativeMessageID(event.OrganizationID, event.RunnerID, "events", event.Type, event.EventID)
			require.NoError(t, err)
			require.Equal(t, expected, message.ID)
			subject, err := hmessaging.RunnerEventSubject(event.OrganizationID, event.RunnerID, event.Type)
			require.NoError(t, err)
			require.Equal(t, subject, message.Subject)
			switch event.Type {
			case "rollout-progress":
				require.NoError(t, f.server.ApplyPipelineProgressEnvelope(f.ctx, event))
			case "deployment-result":
				require.NoError(t, json.Unmarshal(event.Payload, &result))
				if index == 2 {
					wrongRoute := event
					wrongRoute.RunnerID = "wrong-site"
					require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, wrongRoute, result), ErrRunnerEventTargetInvalid)
				}
				require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, result))
				if index == 2 {
					require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, result), "duplicate observation completion must replay")
				}
			default:
				t.Fatal("unexpected event kind")
			}
		}
		saved, _, _, _, err := f.server.Database.GetDeployment(context.Background(), nil, dep.OrgId, dep.Id, model.GetModeDefault)
		require.NoError(t, err)
		readTx, err := f.server.Database.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = readTx.Rollback() }()
		effective, err := model.EffectiveStateDeploymentID(f.ctx, readTx, dep.DeploymentEnvUuid)
		require.NoError(t, err)
		switch index {
		case 0:
			successful = dep.Id
			baselineFingerprint = model.ObservationBaselineFingerprint(model.EncodedDeploymentManifest(`{"workloads":{}}`), model.RawTofu(main), model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), nil, target)
			require.Equal(t, successful, effective)
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p)) FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, successful).Scan(&provenance))
			require.Equal(t, model.DeploymentStatusSucceeded, saved.Status)
			require.NotNil(t, result.Outputs)
			require.Nil(t, result.NoEffectReceipt)
		case 1:
			require.Equal(t, model.DeploymentStatusFailed, saved.Status)
			require.NotNil(t, result.NoEffectReceipt)
			payload, err := model.FrozenPipelinePayload(envelope, dep)
			require.NoError(t, err)
			sum := sha256.Sum256(payload)
			require.Equal(t, hex.EncodeToString(sum[:]), result.NoEffectReceipt.CommandSha256)
			require.Equal(t, successful, effective)
			var unchanged json.RawMessage
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p)) FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, successful).Scan(&unchanged))
			require.JSONEq(t, string(provenance), string(unchanged), "failed command must preserve successful result and effect provenance")
			var base uuid.UUID
			var captured, blocked bool
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT effect_base_deployment_id,effect_base_captured,no_effect_blocked FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&base, &captured, &blocked))
			require.Equal(t, successful, base)
			require.True(t, captured)
			require.False(t, blocked)
		default:
			require.Equal(t, model.DeploymentStatusSucceeded, saved.Status)
			require.False(t, saved.HasPipeline)
			require.NotNil(t, result.ObservationResult)
			require.Nil(t, result.Outputs)
			require.Empty(t, result.Metadata)
			require.Nil(t, result.NoEffectReceipt)
			require.Equal(t, successful, effective)
			var unchanged, retained json.RawMessage
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p)) FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, successful).Scan(&unchanged))
			require.JSONEq(t, string(provenance), string(unchanged), "observation changed successful deployment provenance")
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT result FROM environment_observation_receipts WHERE deployment_id=$1 AND completion_sha256 IS NOT NULL`, dep.Id).Scan(&retained))
			encoded, err := json.Marshal(result.ObservationResult)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), string(retained))
			var pipelines, liveReservations int
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&pipelines))
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, dep.Id).Scan(&liveReservations))
			require.Zero(t, pipelines)
			require.Zero(t, liveReservations)
		}
		require.NoError(t, readTx.Rollback())
		writeNativeProducerFixture(t, filepath.Join(input.Directory, fmt.Sprintf("settled-%d", index)), nil)
	}
}

func writeNativeProducerFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path+".tmp", data, 0600))
	require.NoError(t, os.Rename(path+".tmp", path))
}

//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reuse the signed-request and real PG fixture; the HTTP peer is not Core/IAM.
func newObservationTargetHTTPFixture(t *testing.T, current cp.Runner) (observationRecoveryFixture, *atomic.Int64, *atomic.Bool) {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	base := f.mustDeploy(queueBody(nil))
	f.finish(base.Id, model.DeploymentStatusSucceeded)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	calls, offline := &atomic.Int64{}, &atomic.Bool{}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if offline.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/internal/orgs/"+queueOrg+"/runners/"+queueRunner {
			w.Header().Set("Content-Type", "application/json")
			assert.NoError(t, json.NewEncoder(w).Encode(cp.InternalRunner{OrgId: current.OrgId, Id: current.Id, Uuid: current.Uuid,
				RunnerConfiguration: current.RunnerConfiguration, StateStorageConfiguration: current.StateStorageConfiguration,
				RunnerConfigurationSecret: cp.ConfigurationSecret{Path: "PRIVATE_OBSERVATION_SECRET_REFERENCE", Version: 1}}))
			return
		}
		f.core.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public,
		Admission: ExternalDeploymentAdmissionService{Database: f.store}, ControlPlaneClient: client, EventPublisher: f.server.EventPublisher}
	router := echo.New()
	router.POST(ObservationStartPath, adapter.HandleObservationStart)
	return observationRecoveryFixture{queueFixture: f, router: router, private: private}, calls, offline
}

func TestNativeObservationHTTPNeverAdoptsLegacyBaseline(t *testing.T) {
	current := qualifiedNativeRunner(t)
	current.OrgId, current.Id = queueOrg, queueRunner
	config, err := current.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	protocols := append(*config.SupportedProtocols, "environment-observation/v1")
	config.SupportedProtocols = &protocols
	capabilities := append(*config.Capabilities, "environment-observation/v1")
	config.Capabilities = &capabilities
	require.NoError(t, current.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	f, calls, _ := newObservationTargetHTTPFixture(t, current)
	eligible := true
	f.core.set(func(c *queueCore) { c.nativeFirstEligible = &eligible })
	now := time.Now().UTC()
	body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
		ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
	before := map[string]int{}
	for _, table := range []string{"deployments", "runner_job_reservations", "environment_deployment_leases", "deployment_dispatch_budgets", "pending_event_messages"} {
		before[table] = f.count("SELECT count(*) FROM " + table)
	}
	response := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt observation.ObservationReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, observation.ObservationReceiptDispositionSkipped, receipt.Disposition)
	require.Equal(t, observation.ObservationReceiptReasonBaselineChanged, receipt.Reason)
	require.Nil(t, receipt.RunId)
	require.EqualValues(t, 2, calls.Load(), "one Environment lookup and one internal Runner lookup")
	require.NotContains(t, response.Body.String(), "PRIVATE_OBSERVATION_SECRET_REFERENCE")
	for table, count := range before {
		require.Equal(t, count, f.count("SELECT count(*) FROM "+table), table)
	}
	require.Equal(t, 1, f.count(`SELECT count(*) FROM environment_observation_receipts`))
}

func TestNativeObservationHTTPRequiresCanonicalEligibility(t *testing.T) {
	eligible, ineligible := true, false
	for _, test := range []struct {
		name        string
		eligibility *bool
		capability  bool
	}{
		{"qualified", &eligible, true},
		{"unknown", nil, true},
		{"ineligible", &ineligible, true},
		{"missing capability", &eligible, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := observationRecoveryFixture{queueFixture: newNativeProducerFixture(t)}
			current := qualifiedNativeRunner(t)
			current.OrgId, current.Id = queueOrg, queueRunner
			config, err := current.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
			require.NoError(t, err)
			protocols := append(*config.SupportedProtocols, "environment-observation/v1")
			config.SupportedProtocols = &protocols
			if test.capability {
				capabilities := append(*config.Capabilities, "environment-observation/v1")
				config.Capabilities = &capabilities
			}
			require.NoError(t, current.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
			countedRunnerPeer(t, f.queueFixture, func(int64) (cp.Runner, int) { return current, http.StatusOK })
			base := f.mustDeploy(queueBody(nil))
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, tx, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
			require.NoError(t, err)
			require.NoError(t, model.SettleDeploymentPipeline(f.ctx, tx, base.Id, model.DeploymentStatusSucceeded))
			require.NoError(t, tx.Commit())
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, base.Id))
			baseline := nativeProducerReceipt(t, f.queueFixture, base.Id)
			f.core.set(func(c *queueCore) { c.nativeFirstEligible = test.eligibility })
			calls, offline := &atomic.Int64{}, &atomic.Bool{}
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if offline.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if r.URL.Path == "/internal/orgs/"+queueOrg+"/runners/"+queueRunner {
					w.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(w).Encode(cp.InternalRunner{OrgId: current.OrgId, Id: current.Id, Uuid: current.Uuid,
						RunnerConfiguration: current.RunnerConfiguration, StateStorageConfiguration: current.StateStorageConfiguration,
						RunnerConfigurationSecret: cp.ConfigurationSecret{Path: "PRIVATE_OBSERVATION_SECRET_REFERENCE", Version: 1}}))
					return
				}
				f.core.ServeHTTP(w, r)
			}))
			t.Cleanup(peer.Close)
			client, err := cp.NewClientWithResponses(peer.URL)
			require.NoError(t, err)
			public, private, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			f.private = private
			adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public,
				Admission: ExternalDeploymentAdmissionService{Database: f.store}, ControlPlaneClient: client, EventPublisher: f.server.EventPublisher}
			f.router = echo.New()
			f.router.POST(ObservationStartPath, adapter.HandleObservationStart)
			now := time.Now().UTC()
			body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
				ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
			path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
			publisher := f.server.EventPublisher.(*hmessaging.RecordingPublisher)
			messages, admissions := len(publisher.Messages()), f.core.admissions()
			before := map[string]int{
				"deployments":                   f.count("SELECT count(*) FROM deployments"),
				"runner_job_reservations":       f.count("SELECT count(*) FROM runner_job_reservations"),
				"environment_deployment_leases": f.count("SELECT count(*) FROM environment_deployment_leases"),
				"deployment_dispatch_budgets":   f.count("SELECT count(*) FROM deployment_dispatch_budgets"),
				"pending_event_messages":        f.count("SELECT count(*) FROM pending_event_messages"),
			}
			response := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "PRIVATE_OBSERVATION_SECRET_REFERENCE")
			var receipt observation.ObservationReceipt
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
			if test.eligibility != nil && *test.eligibility && test.capability {
				require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
				require.Equal(t, &base.Id, receipt.BaselineId)
				require.NotNil(t, receipt.RunId)
				require.NotNil(t, receipt.BaselineFingerprint)
				observed, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, *receipt.RunId, model.GetModeDefault)
				require.NoError(t, err)
				require.Equal(t, baseline.NativeTarget, observed.NativeTarget)
				require.Equal(t, model.DeploymentModeObservation, observed.Mode)
				require.False(t, observed.HasPipeline)
				require.False(t, observed.EncryptedLogsRecipient.IsSet())
				require.False(t, observed.EncryptedOutputsRecipient.IsSet())
				require.Equal(t, model.ObservationBaselineFingerprint(manifest, tofu, graph, observed.ModuleArtifacts, observed.NativeTarget), *receipt.BaselineFingerprint)
				require.Equal(t, 1, f.count(`SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, observed.Id))
				require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_dispatch_budgets WHERE deployment_id=$1`, observed.Id))
				require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE deployment_id=$1`, observed.Id))
				require.Equal(t, 1, f.count(`SELECT count(*) FROM environment_deployment_leases WHERE deployment_id=$1 AND environment_id=$2 AND holder_id=$1 AND purpose='deployment' AND expires_at > now()`, observed.Id, f.core.envUUID))
				require.Len(t, publisher.Messages(), messages+1)
			} else {
				require.Equal(t, observation.ObservationReceiptReasonUnsupported, receipt.Reason)
				require.Nil(t, receipt.RunId)
				after := map[string]int{
					"deployments":                   f.count("SELECT count(*) FROM deployments"),
					"runner_job_reservations":       f.count("SELECT count(*) FROM runner_job_reservations"),
					"environment_deployment_leases": f.count("SELECT count(*) FROM environment_deployment_leases"),
					"deployment_dispatch_budgets":   f.count("SELECT count(*) FROM deployment_dispatch_budgets"),
					"pending_event_messages":        f.count("SELECT count(*) FROM pending_event_messages"),
				}
				require.Equal(t, before, after)
				require.Len(t, publisher.Messages(), messages)
			}
			check, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = check.Rollback() }()
			watermark, err := model.LatestNativeAdmissionWatermark(f.ctx, check, queueOrg, f.core.projectUUID, f.core.envUUID)
			require.NoError(t, err)
			require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: baseline.AdmissionID, Epoch: baseline.CoreEpoch}, watermark)
			head, err := model.EffectiveStateDeploymentID(f.ctx, check, f.core.envUUID)
			require.NoError(t, err)
			require.Equal(t, base.Id, head)
			require.NoError(t, check.Rollback())
			require.Equal(t, admissions, f.core.admissions())
			metadataReads := calls.Load()
			require.EqualValues(t, 2, metadataReads)
			offline.Store(true)
			again := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
			require.Equal(t, http.StatusOK, again.Code, again.Body.String())
			require.Equal(t, response.Body.String(), again.Body.String())
			require.Equal(t, metadataReads, calls.Load(), "replay must precede unavailable metadata")
		})
	}
}

func TestObservationHTTPRejectsUnqualifiedAndForeignCurrentRunner(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*cp.Runner)
		status int
	}{
		{"disabled", func(r *cp.Runner) {
			c, err := r.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
			require.NoError(t, err)
			c.NativeExecution, c.SupportedProtocols, c.Capabilities = nil, nil, nil
			require.NoError(t, r.RunnerConfiguration.FromNativeAgentRunnerConfiguration(c))
		}, http.StatusOK},
		{"missing observer", func(*cp.Runner) {}, http.StatusOK},
		{"foreign organization", func(r *cp.Runner) { r.OrgId = "foreign" }, http.StatusConflict},
		{"foreign Runner", func(r *cp.Runner) { r.Id = "foreign" }, http.StatusConflict},
		{"job injection", func(r *cp.Runner) {
			raw, err := json.Marshal(r.RunnerConfiguration)
			require.NoError(t, err)
			var value map[string]any
			require.NoError(t, json.Unmarshal(raw, &value))
			value["supported_protocols"] = []string{"stellwerk.pipeline/v2", "stellwerk.pipeline.plan-apply/v2", "environment-observation/v1"}
			value["capabilities"] = []string{"environment-observation/v1"}
			value["job"] = map[string]any{"environment": map[string]string{"TOKEN": "PRIVATE_JOB_TOKEN"}}
			raw, err = json.Marshal(value)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &r.RunnerConfiguration))
		}, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := qualifiedNativeRunner(t)
			current.OrgId, current.Id = queueOrg, queueRunner
			test.change(&current)
			f, calls, _ := newObservationTargetHTTPFixture(t, current)
			now := time.Now().UTC()
			body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
				ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
			path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
			response := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
			require.Equal(t, test.status, response.Code, response.Body.String())
			require.EqualValues(t, 2, calls.Load())
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
			require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
			require.NotContains(t, response.Body.String(), "PRIVATE_")
			if test.status == http.StatusOK {
				var receipt observation.ObservationReceipt
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
				require.Equal(t, observation.ObservationReceiptReasonUnsupported, receipt.Reason)
				require.Nil(t, receipt.RunId)
			} else {
				require.Zero(t, f.count(`SELECT count(*) FROM environment_observation_receipts`))
			}
		})
	}
}

func TestObservationHTTPAcceptedReplayPrecedesUnavailableMetadata(t *testing.T) {
	current := cp.Runner{OrgId: queueOrg, Id: queueRunner}
	require.NoError(t, json.Unmarshal([]byte(`{"type":"kubernetes-agent","max_parallel_jobs":2,"supported_protocols":["environment-observation/v1"]}`), &current.RunnerConfiguration))
	f, calls, offline := newObservationTargetHTTPFixture(t, current)
	now := time.Now().UTC()
	body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
		ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
	response := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt observation.ObservationReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	_, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, *receipt.RunId, model.GetModeDefault)
	require.NoError(t, err)
	_, _, baselineTofu, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, *receipt.BaselineId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, baselineTofu, tofu)
	offline.Store(true)
	again := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
	require.Equal(t, response.Body.String(), again.Body.String())
	require.EqualValues(t, 2, calls.Load())
	changed := f.serve(f.request(t, path, ObservationStartAction, body, uuid.New()))
	require.Equal(t, http.StatusConflict, changed.Code)
	body.GrantRevision++
	changed = f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
	require.Equal(t, http.StatusConflict, changed.Code)
	require.EqualValues(t, 2, calls.Load())
}

func TestObservationHTTPExpiredRequestDoesNotConsultCurrentTarget(t *testing.T) {
	f, calls, offline := newObservationTargetHTTPFixture(t, cp.Runner{})
	offline.Store(true)
	now := time.Now().UTC()
	body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
		ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now.Add(-time.Minute), AdmissionDeadline: now.Add(-time.Second)}
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
	response := f.serve(f.request(t, path, ObservationStartAction, body, f.actor))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt observation.ObservationReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, observation.ObservationReceiptReasonExpired, receipt.Reason)
	require.Zero(t, calls.Load())
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
}

//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	platformorchestratorcpv3 "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

func TestExternalDeploymentHTTPAdapterCreatesNormalDeploymentAndReauthorizesReplay(t *testing.T) {
	for _, status := range []model.DeploymentStatus{model.DeploymentStatusSucceeded, model.DeploymentStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			testExternalDeploymentHTTPAdapterRollback(t, status, externalRollbackOriginal)
		})
	}
}

func TestExternalRollbackPreservesFirstBaseAcrossExplicitForwardRetry(t *testing.T) {
	for _, status := range []model.DeploymentStatus{model.DeploymentStatusSucceeded, model.DeploymentStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			testExternalDeploymentHTTPAdapterRollback(t, status, externalRollbackForwardRetry)
		})
	}
}

func TestExternalRollbackExplicitlyRetriesOwnTerminalFailedRollback(t *testing.T) {
	for _, status := range []model.DeploymentStatus{model.DeploymentStatusSucceeded, model.DeploymentStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			testExternalDeploymentHTTPAdapterRollback(t, status, externalRollbackFailedRetry)
		})
	}
}

func TestExternalRollbackLegacyReplayAndUnambiguousAdoption(t *testing.T) {
	testExternalDeploymentHTTPAdapterRollback(t, model.DeploymentStatusFailed, externalRollbackLegacy)
}

type externalRollbackScenario uint8

const (
	externalRollbackOriginal externalRollbackScenario = iota
	externalRollbackForwardRetry
	externalRollbackFailedRetry
	externalRollbackLegacy
)

func testExternalDeploymentHTTPAdapterRollback(t *testing.T, forwardStatus model.DeploymentStatus, scenario externalRollbackScenario) {
	t.Helper()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := externalDeploymentTestDatabase(t, ctx, connection)

	now := time.Unix(2_000_000_000, 0)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	operationID := uuid.New()
	actorID := uuid.New()
	projectUUID := uuid.New()
	environmentUUID := uuid.New()
	moduleUUID, baseVersionUUID, targetVersionUUID := uuid.New(), uuid.New(), uuid.New()
	const (
		orgID      = "acme"
		pluginID   = "dev.example.rollouts"
		projectID  = "checkout"
		envID      = "production"
		dispatchID = "dispatch-1"
		runnerID   = "runner-1"
	)
	var checks atomic.Int64
	var grantActive atomic.Bool
	var targetDigest atomic.Value
	grantActive.Store(true)
	targetDigest.Store("")
	callbackEntered := make(chan struct{}, 1)
	callbackRelease := make(chan struct{})

	baseManifest := DeploymentManifest{
		Workloads: map[string]DeploymentManifestWorkload{},
		Shared:    map[string]DeploymentManifestResource{"database": {Type: "postgres"}},
	}
	rawBaseManifest, err := json.Marshal(baseManifest)
	require.NoError(t, err)
	coordinate := platformorchestratorgraph.ResourceCoordinate{Type: "postgres", Class: "default", Id: "shared.database"}
	rawBaseGraph, err := graphs.ToJson(&platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platformorchestratorgraph.ResourceCoordinate]platformorchestratorgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "v1"}},
		},
		Edges:           map[platformorchestratorgraph.ResourceCoordinate]map[string]platformorchestratorgraph.ResourceCoordinate{},
		Workloads:       map[string]platformorchestratorgraph.ResourceCoordinate{},
		SharedResources: map[string]platformorchestratorgraph.ResourceCoordinate{"database": coordinate},
		LeafResources:   map[platformorchestratorgraph.ResourceCoordinate]bool{coordinate: true},
	})
	require.NoError(t, err)
	baseTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	base, err := store.CreateDeployment(ctx, baseTx, orgID, projectID, envID, model.CreateDeploymentParams{
		CreatedBy: actorID, DeploymentEnvUuid: environmentUUID, Mode: model.DeploymentModeDeploy,
		Manifest: rawBaseManifest, Graph: rawBaseGraph, Tofu: model.RawTofu("terraform {}"),
		RunnerId: runnerID, RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Empty[string](),
		ModuleArtifacts: []model.ModuleArtifactRequirement{{
			ModuleID: "postgres", ModuleUUID: moduleUUID, Version: "v1", VersionUUID: baseVersionUUID,
			SemanticVersion: "1.0.0", ArtifactDigest: "sha256:base", Source: "./postgres-v1",
		}},
	})
	require.NoError(t, err)
	require.NoError(t, baseTx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, base.Id, model.UpdateDeploymentStatusAndOutputsParams{
		Status: model.DeploymentStatusSucceeded, StatusMessage: "base ready",
	})
	require.NoError(t, err)

	coreServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/orgs/acme/projects/checkout/envs/production":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"created_at": now, "updated_at": now, "display_name": "Production", "env_type_id": "production",
				"id": envID, "project_id": projectID, "project_uuid": projectUUID, "runner_id": runnerID,
				"status": "active", "uuid": environmentUUID,
			})
		case request.Method == http.MethodPost && request.URL.Path == "/internal/orgs/acme/projects/checkout/envs/production/module-catalogue":
			version, versionUUID, semanticVersion, digest, source := "v2", targetVersionUUID, "2.0.0", targetDigest.Load().(string), ""
			inlineCode := `variable "in" {}`
			if request.Header.Get("X-Stellwerk-Rollback") == "true" {
				if !assert.Empty(t, request.Header.Get("X-Stellwerk-Module-Operation-Id"), "rollback must not consume a pending forward override") {
					response.WriteHeader(http.StatusBadRequest)
					return
				}
				version, versionUUID, semanticVersion, digest, source = "v1", baseVersionUUID, "1.0.0", "sha256:base", "./postgres-v1"
				inlineCode = ""
			} else if request.Header.Get("X-Stellwerk-Module-Operation-Planning") == "true" {
				if !assert.Empty(t, request.Header.Get("X-Stellwerk-Module-Operation-Id"), "planning must not consume a pending Pin override") {
					response.WriteHeader(http.StatusBadRequest)
					return
				}
			} else {
				if !assert.Equal(t, operationID.String(), request.Header.Get("X-Stellwerk-Module-Operation-Id"), "forward execution must bind its exact Pin override operation") {
					response.WriteHeader(http.StatusBadRequest)
					return
				}
			}
			module := map[string]any{
				"org_id": orgID, "id": "postgres", "version_id": version, "resource_type": "postgres",
				"module_source": source, "module_inputs": map[string]any{}, "module_params": map[string]any{},
				"dependencies": map[string]any{}, "coprovisioned": []any{}, "provider_mapping": map[string]string{},
				"rules":      []map[string]any{{"resource_class": "default", "rule_id": uuid.New()}},
				"created_at": now, "updated_at": now,
				"module_uuid": moduleUUID, "version_uuid": versionUUID, "semantic_version": semanticVersion,
				"migration_generation": "generation", "artifact_digest": digest, "semantic_status": "active",
			}
			if inlineCode != "" {
				module["module_source_code"] = inlineCode
			}
			_ = json.NewEncoder(response).Encode(map[string]any{"modules": []map[string]any{module}, "providers": []any{}})
		case request.Method == http.MethodGet && request.URL.Path == "/orgs/acme/runners/runner-1":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"created_at": now, "updated_at": now, "id": runnerID, "org_id": orgID,
				"runner_configuration": map[string]any{
					"type": "kubernetes", "cluster": map[string]any{"auth": map[string]any{}, "cluster_data": map[string]any{"certificate_authority_data": "", "server": ""}},
					"job": map[string]any{"namespace": "runner", "service_account": "runner"},
				},
				"state_storage_configuration": map[string]any{"type": "kubernetes", "namespace": "state"},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/orgs/acme/plugins/dev.example.rollouts/operations/"+operationID.String()+"/check":
			checks.Add(1)
			if !assert.Equal(t, "Bearer dp-service-token", request.Header.Get("Authorization")) {
				response.WriteHeader(http.StatusUnauthorized)
				return
			}
			var check platformorchestratorcpv3.PluginDispatchCheck
			if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&check)) {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			if check.DispatchId == "dispatch-cancel" {
				callbackEntered <- struct{}{}
				<-callbackRelease
			}
			if !grantActive.Load() {
				response.WriteHeader(http.StatusForbidden)
				return
			}
			_ = json.NewEncoder(response).Encode(platformorchestratorcpv3.PluginDispatchAuthorization{
				Action: string(check.Action), ActorId: actorID, DispatchId: check.DispatchId,
				EnvironmentUuid: check.EnvironmentUuid, Fence: check.Fence, OperationId: operationID,
				OrgId: orgID, PluginId: pluginID, ProjectUuid: check.ProjectUuid, RequestDigest: check.RequestDigest,
			})
		default:
			http.Error(response, "unexpected Core test request", http.StatusNotFound)
		}
	}))
	t.Cleanup(coreServer.Close)
	vaultServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/transit/keys/"+externalOperationMaterialTransitKey:
			response.WriteHeader(http.StatusNotFound)
		case request.Method == http.MethodPut && request.URL.Path == "/v1/transit/keys/"+externalOperationMaterialTransitKey:
			var createRequest map[string]any
			if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&createRequest)) {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			if !assert.Equal(t, map[string]any{
				"type": vault.AES256GCM96, "auto_rotate_period": externalOperationMaterialKeyRotationPeriod,
			}, createRequest) {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = response.Write([]byte(`{"data":{}}`))
		case request.Method == http.MethodPut && request.URL.Path == "/v1/transit/encrypt/"+externalOperationMaterialTransitKey:
			var encryptRequest map[string]string
			if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&encryptRequest)) ||
				!assert.NotEmpty(t, encryptRequest["plaintext"]) {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = response.Write([]byte(`{"data":{"ciphertext":"vault:v1:wrapped-operation-identity"}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	t.Cleanup(vaultServer.Close)
	vaultConfig := vaultapi.DefaultConfig()
	vaultConfig.Address = vaultServer.URL
	vaultConfig.HttpClient = vaultServer.Client()
	vaultAPIClient, err := vaultapi.NewClient(vaultConfig)
	require.NoError(t, err)
	vaultAPIClient.SetToken("test-token")
	materialVault := vault.NewVaultClient(vaultAPIClient, zap.NewNop())
	cpClient, err := platformorchestratorcp.NewClientWithResponses(coreServer.URL)
	require.NoError(t, err)
	coreURL, err := url.Parse(coreServer.URL)
	require.NoError(t, err)
	grantClient := CoreExternalDeploymentGrantClient{BaseURL: coreURL, HTTPClient: coreServer.Client(), BearerToken: "dp-service-token"}
	publisher := &hmessaging.RecordingPublisher{}
	adapter := &ExternalDeploymentHTTPAdapter{
		Issuer: "platform-orchestrator-cp", VerificationKey: publicKey,
		Admission:          ExternalDeploymentAdmissionService{Database: store, Authorizer: grantClient, Logger: zap.NewNop()},
		Materials:          ExternalOperationMaterialService{Vault: materialVault},
		ControlPlaneClient: cpClient, EventPublisher: publisher, Logger: zap.NewNop(), Now: func() time.Time { return now },
	}
	server := echo.New()
	server.POST(ExternalDeploymentAdmissionPath, adapter.Handle)
	server.POST(ExternalDeploymentPlanningPath, adapter.HandlePlan)
	server.GET(ExternalDeploymentDispatchReadPath, adapter.HandleDispatchRead)

	planBodyBytes, err := json.Marshal(externalDeploymentPlanBody{
		ProjectID: projectID, EnvID: envID, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID,
		ModuleVersions: map[string]externalModuleSelection{"postgres": {
			ModuleUUID: moduleUUID, Version: "v2", VersionUUID: targetVersionUUID,
			SemanticVersion: "2.0.0", ArtifactDigest: "",
		}},
	})
	require.NoError(t, err)
	planPath := "/orgs/acme/plugins/dev.example.rollouts/external-deployment-plans"
	planClaims := plugincontext.Claims{
		Version: 1, Issuer: "platform-orchestrator-cp", Audience: ExternalDeploymentAudience,
		PluginID: pluginID, OrgID: orgID, ActorID: actorID.String(), OperationID: ExternalDeploymentPlanningAction,
		Method: http.MethodPost, Path: planPath, BodySHA256: plugincontext.Digest(planBodyBytes), RequestID: "plan-1",
		IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix(),
	}
	planAssertion, err := plugincontext.Sign(privateKey, planClaims, now)
	require.NoError(t, err)
	planRequest := httptest.NewRequest(http.MethodPost, planPath, bytes.NewReader(planBodyBytes))
	planRequest.Header.Set("Content-Type", "application/json")
	planRequest.Header.Set("Idempotency-Key", planClaims.RequestID)
	planRequest.Header.Set(plugincontext.Header, planAssertion)
	planResponse := httptest.NewRecorder()
	server.ServeHTTP(planResponse, planRequest)
	require.Equal(t, http.StatusOK, planResponse.Code, planResponse.Body.String())
	var plan externalDeploymentPlanResponse
	require.NoError(t, json.Unmarshal(planResponse.Body.Bytes(), &plan))

	bodyBytes, err := json.Marshal(ExternalDeploymentAdmissionBody{
		OperationRef: operationID, Fence: 7, ProjectId: projectID, EnvId: envID,
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID, Mode: ExternalDeploymentAdmissionBodyModeDeploy,
		Plan: &ExternalDeploymentAdmissionPlan{
			SnapshotId: plan.SnapshotID, Fingerprint: plan.Fingerprint,
			ExpectedModuleVersions: map[string]uuid.UUID{moduleUUID.String(): targetVersionUUID},
		},
	})
	require.NoError(t, err)
	path := "/orgs/acme/plugins/dev.example.rollouts/external-deployments"
	claims := plugincontext.Claims{
		Version: 1, Issuer: "platform-orchestrator-cp", Audience: ExternalDeploymentAudience,
		PluginID: pluginID, OrgID: orgID, ActorID: actorID.String(), OperationID: ExternalDeploymentAction,
		Method: http.MethodPost, Path: path, BodySHA256: plugincontext.Digest(bodyBytes), RequestID: dispatchID,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix(),
	}
	assertion, err := plugincontext.Sign(privateKey, claims, now)
	require.NoError(t, err)
	do := func(body []byte, signed, requestID string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", requestID)
		request.Header.Set(plugincontext.Header, signed)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}

	targetDigest.Store("sha256:changed-after-plan")
	changedPlan := do(bodyBytes, assertion, dispatchID)
	require.Equal(t, http.StatusConflict, changedPlan.Code, changedPlan.Body.String())
	targetDigest.Store("")
	created := do(bodyBytes, assertion, dispatchID)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var createdBody ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &createdBody))
	require.False(t, createdBody.Replayed)
	deployment, _, _, _, err := store.GetDeployment(ctx, nil, orgID, createdBody.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, actorID, deployment.CreatedBy)
	require.Equal(t, model.DeploymentModeDeploy, deployment.Mode)
	require.Equal(t, targetVersionUUID, deployment.ModuleArtifacts[0].VersionUUID)
	require.True(t, deployment.EncryptedOutputsRecipient.IsSet())
	require.Equal(t, deployment.EncryptedOutputsRecipient.Must(), deployment.EncryptedLogsRecipient.Must())
	materialTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	material, err := model.GetExternalOperationMaterial(ctx, materialTx, orgID, pluginID, operationID)
	require.NoError(t, err)
	require.Equal(t, deployment.EncryptedOutputsRecipient.Must(), material.Recipient)
	require.Equal(t, "vault:v1:wrapped-operation-identity", material.EncryptedIdentity)
	require.NoError(t, materialTx.Rollback())

	replayed := do(bodyBytes, assertion, dispatchID)
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	require.EqualValues(t, 3, checks.Load(), "replay must reauthorize before loading its receipt")
	if forwardStatus == model.DeploymentStatusFailed {
		resultServer := Server{Database: store, Logger: zap.NewNop(), EventPublisher: publisher}
		require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, runnerID, deployment.Id, uuid.NewString(), DeploymentResultsUpdateBody{
			Status: Failure, Error: &Error{Error: "controlled_failure", Message: "forward worker stopped after partial effect"},
		}))
	} else {
		_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: forwardStatus, StatusMessage: "forward worker stopped"})
		require.NoError(t, err)
	}
	rollbackRequest := ExternalDeploymentAdmissionBody{
		OperationRef: operationID, Fence: 7, ProjectId: projectID, EnvId: envID,
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID, Mode: ExternalDeploymentAdmissionBodyModeRollback,
		Rollback: &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: deployment.Id},
	}
	for _, test := range []struct {
		name   string
		edit   string
		args   []any
		change func(*ExternalDeploymentAdmissionBody)
		org    string
		plugin string
	}{
		{name: "foreign operation", change: func(body *ExternalDeploymentAdmissionBody) { body.OperationRef = uuid.New() }},
		{name: "foreign organization", org: "foreign"},
		{name: "foreign plugin", plugin: "dev.example.foreign"},
		{name: "foreign project UUID", change: func(body *ExternalDeploymentAdmissionBody) { body.ProjectUuid = uuid.New() }},
		{name: "foreign Environment UUID", change: func(body *ExternalDeploymentAdmissionBody) { body.EnvironmentUuid = uuid.New() }},
		{name: "foreign project slug", change: func(body *ExternalDeploymentAdmissionBody) { body.ProjectId = "foreign" }},
		{name: "foreign Environment slug", change: func(body *ExternalDeploymentAdmissionBody) { body.EnvId = "foreign" }},
		{name: "running forward", edit: "UPDATE deployments SET status='executing' WHERE id=$1", args: []any{deployment.Id}},
		{name: "unknown interrupted forward", edit: "UPDATE deployments SET status='executing',completed_at=NULL WHERE id=$1", args: []any{deployment.Id}},
		{name: "forward not ended", edit: "UPDATE deployments SET completed_at=NULL WHERE id=$1", args: []any{deployment.Id}},
		{name: "watchdog failure without Runner result", edit: "WITH failed AS (UPDATE deployments SET status='failed' WHERE id=$1 RETURNING id) DELETE FROM runner_event_inbox WHERE deployment_id=(SELECT id FROM failed)", args: []any{deployment.Id}},
		{name: "foreign Runner result", edit: "WITH failed AS (UPDATE deployments SET status='failed' WHERE id=$1 RETURNING id) UPDATE runner_event_inbox SET runner_id='foreign' WHERE deployment_id=(SELECT id FROM failed)", args: []any{deployment.Id}},
		{name: "foreign organization result", edit: "WITH failed AS (UPDATE deployments SET status='failed' WHERE id=$1 RETURNING id) UPDATE runner_event_inbox SET org_id='foreign' WHERE deployment_id=(SELECT id FROM failed)", args: []any{deployment.Id}},
		{name: "different Deployment result", edit: "WITH failed AS (UPDATE deployments SET status='failed' WHERE id=$1 RETURNING id) UPDATE runner_event_inbox SET deployment_id=$2 WHERE deployment_id=(SELECT id FROM failed)", args: []any{deployment.Id, uuid.New()}},
		{name: "runner-error is not execution completion", edit: "WITH failed AS (UPDATE deployments SET status='failed' WHERE id=$1 RETURNING id) UPDATE runner_event_inbox SET event_type='runner-error' WHERE deployment_id=(SELECT id FROM failed)", args: []any{deployment.Id}},
		{name: "missing captured base", edit: "UPDATE external_operation_deployments SET base_deployment_id=NULL WHERE deployment_id=$1", args: []any{deployment.Id}},
		{name: "missing retained base", edit: "UPDATE external_operation_deployments SET base_deployment_id=$1 WHERE deployment_id=$2", args: []any{uuid.New(), deployment.Id}},
		{name: "failed base", edit: "UPDATE deployments SET status='failed' WHERE id=$1", args: []any{base.Id}},
		{name: "base not ended", edit: "UPDATE deployments SET completed_at=NULL WHERE id=$1", args: []any{base.Id}},
		{name: "base missing Tofu", edit: "UPDATE deployments SET tofu='' WHERE id=$1", args: []any{base.Id}},
		{name: "base missing manifest", edit: "UPDATE deployments SET manifest='null' WHERE id=$1", args: []any{base.Id}},
		{name: "base missing graph", edit: "UPDATE deployments SET graph='null' WHERE id=$1", args: []any{base.Id}},
		{name: "newer head", edit: "UPDATE deployment_environments SET last_state_deployment_id=$1 WHERE id=$2", args: []any{base.Id, environmentUUID}},
		{name: "active mutating lease", edit: "INSERT INTO environment_deployment_leases (environment_id,org_id,project_id,env_id,holder_id,purpose,deployment_id,expires_at) VALUES ($1,$2,$3,$4,$5,'deployment',$5,now()+interval '1 hour')", args: []any{environmentUUID, orgID, projectID, envID, uuid.New()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			if test.edit != "" {
				_, err = tx.ExecContext(ctx, test.edit, test.args...)
				require.NoError(t, err)
			}
			body := rollbackRequest
			if test.change != nil {
				test.change(&body)
			}
			org, plugin := orgID, pluginID
			if test.org != "" {
				org = test.org
			}
			if test.plugin != "" {
				plugin = test.plugin
			}
			_, err = adapter.createExternalRollback(ctx, tx, actorID, org, plugin, body, platformorchestratorcp.Environment{Uuid: environmentUUID, ProjectId: projectID, ProjectUuid: &projectUUID, Id: envID, RunnerId: ref.Ref(runnerID)}, material.Recipient)
			require.Error(t, err)
			var count int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM deployments WHERE mode='rollback'").Scan(&count))
			require.Zero(t, count, "rejected recovery must create no rollback Deployment")
		})
	}
	rollbackBody, err := json.Marshal(ExternalDeploymentAdmissionBody{
		OperationRef: operationID, Fence: 7, ProjectId: projectID, EnvId: envID,
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID, Mode: ExternalDeploymentAdmissionBodyModeRollback,
		Rollback: &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: deployment.Id},
	})
	require.NoError(t, err)
	rollbackClaims := claims
	rollbackClaims.RequestID = "dispatch-rollback"
	rollbackClaims.BodySHA256 = plugincontext.Digest(rollbackBody)
	rollbackAssertion, err := plugincontext.Sign(privateKey, rollbackClaims, now)
	require.NoError(t, err)
	rollbackResponse := do(rollbackBody, rollbackAssertion, rollbackClaims.RequestID)
	require.Equal(t, http.StatusCreated, rollbackResponse.Code, rollbackResponse.Body.String())
	var rollbackResult ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(rollbackResponse.Body.Bytes(), &rollbackResult))
	rollbackDeployment, _, _, _, err := store.GetDeployment(ctx, nil, orgID, rollbackResult.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentModeRollback, rollbackDeployment.Mode)
	require.Equal(t, base.Id, rollbackDeployment.RollbackToId.Must(), "rollback target must be the DP-captured effective base")
	require.Equal(t, baseVersionUUID, rollbackDeployment.ModuleArtifacts[0].VersionUUID)
	require.Equal(t, material.Recipient, rollbackDeployment.EncryptedOutputsRecipient.Must())
	require.EqualValues(t, 4, checks.Load())
	rollbackReplay := do(rollbackBody, rollbackAssertion, rollbackClaims.RequestID)
	require.Equal(t, http.StatusOK, rollbackReplay.Code, rollbackReplay.Body.String())
	var replayedRollback ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(rollbackReplay.Body.Bytes(), &replayedRollback))
	require.True(t, replayedRollback.Replayed)
	require.Equal(t, rollbackResult.DeploymentId, replayedRollback.DeploymentId)
	require.EqualValues(t, 5, checks.Load(), "rollback replay must reauthorize and retain one normal Deployment even after its head changes")
	fixture := externalRollbackFixture{
		grantActive:        &grantActive,
		checks:             &checks,
		bodyBytes:          bodyBytes,
		assertion:          assertion,
		server:             server,
		ctx:                ctx,
		store:              store,
		adapter:            adapter,
		publisher:          publisher,
		orgID:              orgID,
		pluginID:           pluginID,
		runnerID:           runnerID,
		projectID:          projectID,
		envID:              envID,
		actorID:            actorID,
		operationID:        operationID,
		deployment:         deployment,
		rollbackDeployment: rollbackDeployment,
		base:               base,
		projectUUID:        projectUUID,
		environmentUUID:    environmentUUID,
		rollbackRequest:    rollbackRequest,
		rollbackBody:       rollbackBody,
		rollbackAssertion:  rollbackAssertion,
		rollbackClaims:     rollbackClaims,
		claims:             claims,
		privateKey:         privateKey,
		now:                now,
		material:           material,
		planClaims:         planClaims,
		planPath:           planPath,
		planBodyBytes:      planBodyBytes,
		plan:               plan,
		moduleUUID:         moduleUUID,
		targetVersionUUID:  targetVersionUUID,
		baseVersionUUID:    baseVersionUUID,
		do:                 do,
	}
	if scenario != externalRollbackOriginal {
		switch scenario {
		case externalRollbackLegacy:
			verifyExternalRollbackLegacy(t, fixture)
		case externalRollbackFailedRetry:
			verifyExternalFailedRollbackRetry(t, fixture)
		case externalRollbackForwardRetry:
			verifyExternalForwardRetryBase(t, fixture)
		default:
			t.Fatal("unknown rollback fixture scenario")
		}
		return
	}
	verifyExternalRollbackCannotImplicitlyRetry(t, fixture)

	cancelClaims := claims
	cancelClaims.RequestID = "dispatch-cancel"
	cancelAssertion, err := plugincontext.Sign(privateKey, cancelClaims, now)
	require.NoError(t, err)
	admissionDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { admissionDone <- do(bodyBytes, cancelAssertion, cancelClaims.RequestID) }()
	select {
	case <-callbackEntered:
	case <-ctx.Done():
		t.Fatal("cancellation admission did not reach Core callback")
	}
	grantActive.Store(false)
	dispatchPath := "/orgs/acme/plugins/dev.example.rollouts/projects/" + projectUUID.String() + "/envs/" + environmentUUID.String() + "/external-deployment-dispatches/" + cancelClaims.RequestID
	readClaims := claims
	readClaims.OperationID = ExternalDeploymentReadAction
	readClaims.Method = http.MethodGet
	readClaims.Path = dispatchPath
	readClaims.BodySHA256 = plugincontext.Digest(nil)
	readClaims.RequestID = "read-cancelled-dispatch"
	readAssertion, err := plugincontext.Sign(privateKey, readClaims, now)
	require.NoError(t, err)
	readDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodGet, dispatchPath, nil)
		request.Header.Set(plugincontext.Header, readAssertion)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		readDone <- response
	}()
	select {
	case response := <-readDone:
		t.Fatalf("receipt read returned %d before the in-flight admission resolved", response.Code)
	case <-time.After(100 * time.Millisecond):
	}
	close(callbackRelease)
	select {
	case response := <-admissionDone:
		require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	case <-ctx.Done():
		t.Fatal("cancelled admission did not return")
	}
	select {
	case response := <-readDone:
		require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	case <-ctx.Done():
		t.Fatal("receipt read did not observe authoritative absence")
	}

	verifyExternalAdmissionRevocationAndAssertionBinding(t, fixture)
}

type externalRollbackFixture struct {
	grantActive        *atomic.Bool
	checks             *atomic.Int64
	bodyBytes          []byte
	assertion          string
	server             *echo.Echo
	ctx                context.Context
	store              model.Databaser
	adapter            *ExternalDeploymentHTTPAdapter
	publisher          *hmessaging.RecordingPublisher
	orgID              string
	pluginID           string
	runnerID           string
	projectID          string
	envID              string
	actorID            uuid.UUID
	operationID        uuid.UUID
	deployment         *model.DeploymentSummary
	rollbackDeployment *model.DeploymentSummary
	base               *model.DeploymentSummary
	projectUUID        uuid.UUID
	environmentUUID    uuid.UUID
	rollbackRequest    ExternalDeploymentAdmissionBody
	rollbackBody       []byte
	rollbackAssertion  string
	rollbackClaims     plugincontext.Claims
	claims             plugincontext.Claims
	privateKey         ed25519.PrivateKey
	now                time.Time
	material           *model.ExternalOperationMaterial
	planClaims         plugincontext.Claims
	planPath           string
	planBodyBytes      []byte
	plan               externalDeploymentPlanResponse
	moduleUUID         uuid.UUID
	targetVersionUUID  uuid.UUID
	baseVersionUUID    uuid.UUID
	do                 func([]byte, string, string) *httptest.ResponseRecorder
}

func verifyExternalRollbackLegacy(t *testing.T, fixture externalRollbackFixture) {
	t.Helper()
	ctx := fixture.ctx
	store := fixture.store
	adapter := fixture.adapter
	orgID := fixture.orgID
	pluginID := fixture.pluginID
	deployment := fixture.deployment
	rollbackDeployment := fixture.rollbackDeployment
	base := fixture.base
	projectUUID := fixture.projectUUID
	environmentUUID := fixture.environmentUUID
	rollbackRequest := fixture.rollbackRequest
	rollbackBody := fixture.rollbackBody
	rollbackAssertion := fixture.rollbackAssertion
	rollbackClaims := fixture.rollbackClaims
	do := fixture.do
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DELETE FROM external_operation_rollback_anchors")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE external_operation_deployments SET forward_deployment_id=NULL,retry_of_rollback_deployment_id=NULL WHERE deployment_id=$1", rollbackDeployment.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	legacyReplay := do(rollbackBody, rollbackAssertion, rollbackClaims.RequestID)
	require.Equal(t, http.StatusOK, legacyReplay.Code, legacyReplay.Body.String(), "unknown legacy predecessor must not invalidate old exact-key replay")
	var replay ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(legacyReplay.Body.Bytes(), &replay))
	require.Equal(t, rollbackDeployment.Id, replay.DeploymentId)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	anchor, err := adapter.operationRollbackAnchor(ctx, tx, externalRollbackScope(orgID, pluginID, rollbackRequest), true)
	require.NoError(t, err)
	require.Equal(t, deployment.Id, anchor.FirstForwardDeploymentID)
	require.Equal(t, base.Id, anchor.RollbackBaseDeploymentID)
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, orgID, pluginID, rollbackDeployment.Id)
	require.NoError(t, err)
	status, err := adapter.projectExternalDeploymentStatus(ctx, tx, *rollbackDeployment, *receipt, projectUUID, environmentUUID)
	require.NoError(t, err)
	require.Equal(t, deployment.Id, *status.ForwardDeploymentID)
	require.Equal(t, deployment.Id, *status.AdmissionBaseDeploymentID)
	require.Equal(t, base.Id, *status.RollbackBaseDeploymentID)
	require.False(t, receipt.ForwardDeploymentID.IsSet(), "read projection must not persist guessed retry authority")
	require.NotNil(t, status.RollbackRetryProvenanceRecorded)
	require.False(t, *status.RollbackRetryProvenanceRecorded)
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	t.Logf("LegacyR1Status=%s", encoded)
	forward, _, _, _, err := store.GetDeployment(ctx, tx, orgID, deployment.Id, model.GetModeDefault)
	require.NoError(t, err)
	forwardReceipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, orgID, pluginID, forward.Id)
	require.NoError(t, err)
	forwardStatus, err := adapter.projectExternalDeploymentStatus(ctx, tx, *forward, *forwardReceipt, projectUUID, environmentUUID)
	require.NoError(t, err)
	require.Equal(t, deployment.Id, *forwardStatus.ForwardDeploymentID)
	require.Equal(t, base.Id, *forwardStatus.AdmissionBaseDeploymentID)
	require.Nil(t, forwardStatus.RollbackRetryProvenanceRecorded)
	encoded, err = json.Marshal(forwardStatus)
	require.NoError(t, err)
	t.Logf("LegacyF1Status=%s", encoded)
	require.NoError(t, tx.Commit())
	for _, test := range []struct {
		name, edit string
		args       []any
	}{
		{name: "second legacy rollback is not guessed", edit: `INSERT INTO external_operation_deployments
			(org_id,plugin_id,operation_ref,fence,request_id,assertion_action,service_action,request_body_sha256,actor_id,project_id,env_id,deployment_id,created_at,project_uuid,environment_uuid,plan_snapshot_id,base_deployment_id,forward_deployment_id,retry_of_rollback_deployment_id)
			SELECT org_id,plugin_id,operation_ref,fence,request_id||'-other',assertion_action,service_action,request_body_sha256,actor_id,project_id,env_id,$2,created_at,project_uuid,environment_uuid,plan_snapshot_id,base_deployment_id,forward_deployment_id,retry_of_rollback_deployment_id
			FROM external_operation_deployments WHERE deployment_id=$1`, args: []any{rollbackDeployment.Id, uuid.New()}},
		{name: "failed forward outcome proof missing", edit: "DELETE FROM runner_event_inbox WHERE deployment_id=$1", args: []any{deployment.Id}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			_, err = tx.ExecContext(ctx, test.edit, test.args...)
			require.NoError(t, err)
			projected, err := adapter.projectExternalDeploymentStatus(ctx, tx, *rollbackDeployment, *receipt, projectUUID, environmentUUID)
			require.NoError(t, err)
			require.Nil(t, projected.ForwardDeploymentID)
			require.Nil(t, projected.AdmissionBaseDeploymentID)
		})
	}
	resultServer := Server{Database: store, Logger: zap.NewNop(), EventPublisher: fixture.publisher}
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, fixture.runnerID, rollbackDeployment.Id, uuid.NewString(), DeploymentResultsUpdateBody{Status: Failure}))
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	candidate := rollbackRequest
	candidate.FirstForwardDeploymentId = &deployment.Id
	candidate.Rollback = &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: deployment.Id, RetryOfRollbackDeploymentId: &rollbackDeployment.Id}
	_, err = adapter.createExternalRollback(ctx, tx, fixture.actorID, orgID, pluginID, candidate,
		platformorchestratorcp.Environment{Uuid: environmentUUID, ProjectId: fixture.projectID, ProjectUuid: &projectUUID, Id: fixture.envID, RunnerId: ref.Ref(fixture.runnerID)}, fixture.material.Recipient)
	require.ErrorContains(t, err, "predecessor", "projected legacy completion evidence must not authorize a new retry without durable provenance")
	var rollbacks int
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM deployments WHERE mode='rollback'").Scan(&rollbacks))
	require.Equal(t, 1, rollbacks)
}

func verifyExternalFailedRollbackRetry(t *testing.T, fixture externalRollbackFixture) {
	t.Helper()
	ctx := fixture.ctx
	store := fixture.store
	adapter := fixture.adapter
	publisher := fixture.publisher
	orgID := fixture.orgID
	pluginID := fixture.pluginID
	runnerID := fixture.runnerID
	projectID := fixture.projectID
	envID := fixture.envID
	actorID := fixture.actorID
	deployment := fixture.deployment
	rollbackDeployment := fixture.rollbackDeployment
	base := fixture.base
	projectUUID := fixture.projectUUID
	environmentUUID := fixture.environmentUUID
	rollbackRequest := fixture.rollbackRequest
	claims := fixture.claims
	privateKey := fixture.privateKey
	now := fixture.now
	material := fixture.material
	do := fixture.do
	resultServer := Server{Database: store, Logger: zap.NewNop(), EventPublisher: publisher}
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, runnerID, rollbackDeployment.Id, uuid.NewString(), DeploymentResultsUpdateBody{
		Status: Failure, Error: &Error{Error: "controlled_failure", Message: "rollback worker stopped after partial effect"},
	}))
	retryRequest := rollbackRequest
	retryRequest.FirstForwardDeploymentId = &deployment.Id
	retryRequest.Rollback = &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: deployment.Id, RetryOfRollbackDeploymentId: &rollbackDeployment.Id}
	for _, test := range []struct {
		name, edit string
		args       []any
		change     func(*ExternalDeploymentAdmissionBody)
	}{
		{name: "no explicit predecessor", change: func(body *ExternalDeploymentAdmissionBody) { body.Rollback.RetryOfRollbackDeploymentId = nil }},
		{name: "foreign predecessor", change: func(body *ExternalDeploymentAdmissionBody) {
			body.Rollback.RetryOfRollbackDeploymentId = ref.Ref(uuid.New())
		}},
		{name: "later anchor cannot reset base", change: func(body *ExternalDeploymentAdmissionBody) { body.FirstForwardDeploymentId = &rollbackDeployment.Id }},
		{name: "wrong operation", change: func(body *ExternalDeploymentAdmissionBody) { body.OperationRef = uuid.New() }},
		{name: "wrong Environment", change: func(body *ExternalDeploymentAdmissionBody) { body.EnvironmentUuid = uuid.New() }},
		{name: "missing result", edit: "DELETE FROM runner_event_inbox WHERE deployment_id=$1", args: []any{rollbackDeployment.Id}},
		{name: "wrong Runner result", edit: "UPDATE runner_event_inbox SET runner_id='other-runner' WHERE deployment_id=$1", args: []any{rollbackDeployment.Id}},
		{name: "wrong org result", edit: "UPDATE runner_event_inbox SET org_id='other-org' WHERE deployment_id=$1", args: []any{rollbackDeployment.Id}},
		{name: "unknown outcome", edit: "UPDATE deployments SET status='executing' WHERE id=$1", args: []any{rollbackDeployment.Id}},
		{name: "uncompleted predecessor", edit: "UPDATE deployments SET completed_at=NULL WHERE id=$1", args: []any{rollbackDeployment.Id}},
		{name: "unknown legacy provenance", edit: "UPDATE external_operation_deployments SET forward_deployment_id=NULL WHERE deployment_id=$1", args: []any{rollbackDeployment.Id}},
		{name: "different original forward", edit: "UPDATE external_operation_deployments SET forward_deployment_id=$2 WHERE deployment_id=$1", args: []any{rollbackDeployment.Id, uuid.New()}},
		{name: "different base", edit: "UPDATE deployments SET rollback_to_id=$2 WHERE id=$1", args: []any{rollbackDeployment.Id, base.Id}},
		{name: "foreign newer head", edit: "UPDATE deployment_environments SET last_state_deployment_id=$2 WHERE id=$1", args: []any{environmentUUID, deployment.Id}},
		{name: "active lease", edit: "INSERT INTO environment_deployment_leases (environment_id,org_id,project_id,env_id,holder_id,purpose,expires_at) VALUES ($1,'acme','checkout','production',$2,'deployment',now()+interval '1 minute')", args: []any{environmentUUID, uuid.New()}},
	} {
		if test.name == "different base" {
			test.args[1] = uuid.New()
		}
		t.Run(test.name, func(t *testing.T) {
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			if test.edit != "" {
				_, err = tx.ExecContext(ctx, test.edit, test.args...)
				require.NoError(t, err)
			}
			candidate := retryRequest
			rollback := *retryRequest.Rollback
			candidate.Rollback = &rollback
			if test.change != nil {
				test.change(&candidate)
			}
			_, err = adapter.createExternalRollback(ctx, tx, actorID, orgID, pluginID, candidate, platformorchestratorcp.Environment{Uuid: environmentUUID, ProjectId: projectID, ProjectUuid: &projectUUID, Id: envID, RunnerId: ref.Ref(runnerID)}, material.Recipient)
			require.Error(t, err)
			var count int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM deployments WHERE mode='rollback'").Scan(&count))
			require.Equal(t, 1, count, "rejected retry must create no Deployment")
		})
	}
	retryBody, err := json.Marshal(retryRequest)
	require.NoError(t, err)
	retryClaims := claims
	retryClaims.RequestID, retryClaims.BodySHA256 = "dispatch-explicit-rollback-retry", plugincontext.Digest(retryBody)
	retryAssertion, err := plugincontext.Sign(privateKey, retryClaims, now)
	require.NoError(t, err)
	created := do(retryBody, retryAssertion, retryClaims.RequestID)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var retryResult ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &retryResult))
	retryDeployment, _, _, _, err := store.GetDeployment(ctx, nil, orgID, retryResult.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, base.Id, retryDeployment.RollbackToId.Must())
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, orgID, pluginID, retryDeployment.Id)
	require.NoError(t, err)
	require.Equal(t, deployment.Id, receipt.ForwardDeploymentID.Must())
	require.Equal(t, rollbackDeployment.Id, receipt.RetryOfRollbackDeploymentID.Must())
	status, err := adapter.projectExternalDeploymentStatus(ctx, tx, *retryDeployment, *receipt, projectUUID, environmentUUID)
	require.NoError(t, err)
	require.Equal(t, deployment.Id, *status.FirstForwardDeploymentID)
	require.Equal(t, base.Id, *status.RollbackBaseDeploymentID)
	require.Equal(t, rollbackDeployment.Id, *status.AdmissionBaseDeploymentID)
	require.NotNil(t, status.RollbackRetryProvenanceRecorded)
	require.True(t, *status.RollbackRetryProvenanceRecorded)
	require.NoError(t, tx.Rollback())
	verifyExternalRollbackProvenanceMarker(t, fixture, *retryDeployment)
	replayed := do(retryBody, retryAssertion, retryClaims.RequestID)
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	var replay ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(replayed.Body.Bytes(), &replay))
	require.True(t, replay.Replayed)
	require.Equal(t, retryDeployment.Id, replay.DeploymentId)
}

func verifyExternalRollbackProvenanceMarker(t *testing.T, fixture externalRollbackFixture, retry model.DeploymentSummary) {
	t.Helper()
	for _, test := range []struct {
		name, edit string
		id         uuid.UUID
		args       []any
	}{
		{name: "recorded original", id: fixture.rollbackDeployment.Id},
		{name: "recorded retry", id: retry.Id},
		{name: "missing forward", id: retry.Id, edit: "UPDATE external_operation_deployments SET forward_deployment_id=NULL WHERE deployment_id=$1", args: []any{retry.Id}},
		{name: "unknown forward", id: retry.Id, edit: "UPDATE external_operation_deployments SET forward_deployment_id=$2 WHERE deployment_id=$1", args: []any{retry.Id, uuid.New()}},
		{name: "unknown predecessor", id: retry.Id, edit: "UPDATE external_operation_deployments SET retry_of_rollback_deployment_id=$2 WHERE deployment_id=$1", args: []any{retry.Id, uuid.New()}},
		{name: "predecessor without recorded forward", id: retry.Id, edit: "UPDATE external_operation_deployments SET forward_deployment_id=NULL WHERE deployment_id=$1", args: []any{fixture.rollbackDeployment.Id}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := fixture.store.BeginTx(fixture.ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			if test.edit != "" {
				_, err = tx.ExecContext(fixture.ctx, test.edit, test.args...)
				require.NoError(t, err)
			}
			deployment, _, _, _, err := fixture.store.GetDeployment(fixture.ctx, tx, fixture.orgID, test.id, model.GetModeDefault)
			require.NoError(t, err)
			receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(fixture.ctx, tx, fixture.orgID, fixture.pluginID, test.id)
			require.NoError(t, err)
			status, err := fixture.adapter.projectExternalDeploymentStatus(fixture.ctx, tx, *deployment, *receipt, fixture.projectUUID, fixture.environmentUUID)
			require.NoError(t, err)
			require.NotNil(t, status.RollbackRetryProvenanceRecorded)
			require.Equal(t, test.edit == "", *status.RollbackRetryProvenanceRecorded)
			if test.name == "recorded retry" {
				encoded, err := json.Marshal(status)
				require.NoError(t, err)
				t.Logf("RecordedR2Status=%s", encoded)
			}
		})
	}
}

func verifyExternalForwardRetryBase(t *testing.T, fixture externalRollbackFixture) {
	t.Helper()
	server := fixture.server
	ctx := fixture.ctx
	store := fixture.store
	adapter := fixture.adapter
	publisher := fixture.publisher
	orgID := fixture.orgID
	pluginID := fixture.pluginID
	runnerID := fixture.runnerID
	projectID := fixture.projectID
	envID := fixture.envID
	operationID := fixture.operationID
	deployment := fixture.deployment
	rollbackDeployment := fixture.rollbackDeployment
	base := fixture.base
	projectUUID := fixture.projectUUID
	environmentUUID := fixture.environmentUUID
	rollbackRequest := fixture.rollbackRequest
	claims := fixture.claims
	privateKey := fixture.privateKey
	now := fixture.now
	planClaims := fixture.planClaims
	planPath := fixture.planPath
	planBodyBytes := fixture.planBodyBytes
	plan := fixture.plan
	moduleUUID := fixture.moduleUUID
	targetVersionUUID := fixture.targetVersionUUID
	baseVersionUUID := fixture.baseVersionUUID
	do := fixture.do
	resultServer := Server{Database: store, Logger: zap.NewNop(), EventPublisher: publisher}
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, runnerID, rollbackDeployment.Id, uuid.NewString(), DeploymentResultsUpdateBody{Status: Success}))

	// A forward retry needs a fresh admission snapshot of R1, not B0.
	retryPlanClaims := planClaims
	retryPlanClaims.RequestID = "plan-forward-retry"
	retryPlanAssertion, signErr := plugincontext.Sign(privateKey, retryPlanClaims, now)
	require.NoError(t, signErr)
	retryPlanRequest := httptest.NewRequest(http.MethodPost, planPath, bytes.NewReader(planBodyBytes))
	retryPlanRequest.Header.Set("Content-Type", "application/json")
	retryPlanRequest.Header.Set("Idempotency-Key", retryPlanClaims.RequestID)
	retryPlanRequest.Header.Set(plugincontext.Header, retryPlanAssertion)
	retryPlanResponse := httptest.NewRecorder()
	server.ServeHTTP(retryPlanResponse, retryPlanRequest)
	require.Equal(t, http.StatusOK, retryPlanResponse.Code, retryPlanResponse.Body.String())
	var retryPlan externalDeploymentPlanResponse
	require.NoError(t, json.Unmarshal(retryPlanResponse.Body.Bytes(), &retryPlan))
	require.NotEqual(t, plan.SnapshotID, retryPlan.SnapshotID)

	retryBody, marshalErr := json.Marshal(ExternalDeploymentAdmissionBody{
		OperationRef: operationID, Fence: 7, ProjectId: projectID, EnvId: envID,
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID, Mode: ExternalDeploymentAdmissionBodyModeDeploy,
		Plan: &ExternalDeploymentAdmissionPlan{
			SnapshotId: retryPlan.SnapshotID, Fingerprint: retryPlan.Fingerprint,
			ExpectedModuleVersions: map[string]uuid.UUID{moduleUUID.String(): targetVersionUUID},
		},
	})
	require.NoError(t, marshalErr)
	var invalidRetry ExternalDeploymentAdmissionBody
	require.NoError(t, json.Unmarshal(retryBody, &invalidRetry))
	invalidRetry.FirstForwardDeploymentId = &rollbackDeployment.Id
	invalidBody, marshalErr := json.Marshal(invalidRetry)
	require.NoError(t, marshalErr)
	invalidClaims := claims
	invalidClaims.RequestID, invalidClaims.BodySHA256 = "dispatch-reset-anchor", plugincontext.Digest(invalidBody)
	invalidAssertion, signErr := plugincontext.Sign(privateKey, invalidClaims, now)
	require.NoError(t, signErr)
	require.Equal(t, http.StatusConflict, do(invalidBody, invalidAssertion, invalidClaims.RequestID).Code, "later same-operation forward cannot reset its original base")
	retryClaims := claims
	retryClaims.RequestID = "dispatch-forward-retry"
	retryClaims.BodySHA256 = plugincontext.Digest(retryBody)
	retryAssertion, signErr := plugincontext.Sign(privateKey, retryClaims, now)
	require.NoError(t, signErr)
	retryResponse := do(retryBody, retryAssertion, retryClaims.RequestID)
	require.Equal(t, http.StatusCreated, retryResponse.Code, retryResponse.Body.String())
	var retriedForward ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(retryResponse.Body.Bytes(), &retriedForward))
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, runnerID, retriedForward.DeploymentId, uuid.NewString(), DeploymentResultsUpdateBody{Status: Success}))
	receiptTx, beginErr := store.BeginTx(ctx, nil)
	require.NoError(t, beginErr)
	forwardRetryReceipt, receiptErr := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, receiptTx, orgID, pluginID, retriedForward.DeploymentId)
	require.NoError(t, receiptErr)
	removed, deleteErr := receiptTx.ExecContext(ctx, "DELETE FROM external_deployment_plan_snapshots WHERE org_id=$1 AND plugin_id=$2 AND environment_uuid=$3", orgID, pluginID, environmentUUID)
	require.NoError(t, deleteErr)
	removedCount, countErr := removed.RowsAffected()
	require.NoError(t, countErr)
	require.EqualValues(t, 2, removedCount)
	settledForward, _, _, _, readErr := store.GetDeployment(ctx, receiptTx, orgID, retriedForward.DeploymentId, model.GetModeDefault)
	require.NoError(t, readErr)
	status, statusErr := adapter.projectExternalDeploymentStatus(ctx, receiptTx, *settledForward, *forwardRetryReceipt, projectUUID, environmentUUID)
	require.NoError(t, statusErr)
	require.Equal(t, deployment.Id, *status.FirstForwardDeploymentID)
	require.Equal(t, base.Id, *status.RollbackBaseDeploymentID)
	require.Equal(t, rollbackDeployment.Id, *status.AdmissionBaseDeploymentID)
	require.NoError(t, receiptTx.Commit())
	forwardReplay := do(retryBody, retryAssertion, retryClaims.RequestID)
	require.Equal(t, http.StatusOK, forwardReplay.Code, forwardReplay.Body.String(), "exact-key replay remains valid after candidate plan snapshots are superseded")
	var forwardReplayResult ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(forwardReplay.Body.Bytes(), &forwardReplayResult))
	require.Equal(t, retriedForward.DeploymentId, forwardReplayResult.DeploymentId)
	require.Equal(t, rollbackDeployment.Id, forwardRetryReceipt.BaseDeploymentID.Must(), "F2 must admit against the fresh current head R1")
	require.Equal(t, operationID.String(), forwardRetryReceipt.OperationRef)
	require.Equal(t, environmentUUID, forwardRetryReceipt.EnvironmentUUID.Must())
	t.Run("ambiguous legacy history cannot reset anchor", func(t *testing.T) {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback()) }()
		_, err = tx.ExecContext(ctx, "DELETE FROM external_operation_rollback_anchors")
		require.NoError(t, err)
		_, err = adapter.operationRollbackAnchor(ctx, tx, externalRollbackScope(orgID, pluginID, rollbackRequest), true)
		require.ErrorContains(t, err, "ambiguous")
	})

	retryRollbackBody, marshalErr := json.Marshal(ExternalDeploymentAdmissionBody{
		OperationRef: operationID, Fence: 7, ProjectId: projectID, EnvId: envID,
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID, Mode: ExternalDeploymentAdmissionBodyModeRollback,
		Rollback: &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: retriedForward.DeploymentId},
	})
	require.NoError(t, marshalErr)
	retryRollbackClaims := claims
	retryRollbackClaims.RequestID = "dispatch-rollback-after-forward-retry"
	retryRollbackClaims.BodySHA256 = plugincontext.Digest(retryRollbackBody)
	retryRollbackAssertion, signErr := plugincontext.Sign(privateKey, retryRollbackClaims, now)
	require.NoError(t, signErr)
	retryRollbackResponse := do(retryRollbackBody, retryRollbackAssertion, retryRollbackClaims.RequestID)
	require.Equal(t, http.StatusCreated, retryRollbackResponse.Code, retryRollbackResponse.Body.String())
	var retriedRollback ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(retryRollbackResponse.Body.Bytes(), &retriedRollback))
	secondRollback, _, _, _, readErr := store.GetDeployment(ctx, nil, orgID, retriedRollback.DeploymentId, model.GetModeDefault)
	require.NoError(t, readErr)
	require.Equal(t, model.DeploymentModeRollback, secondRollback.Mode)
	require.Equal(t, baseVersionUUID, secondRollback.ModuleArtifacts[0].VersionUUID, "equivalent Module Versions are insufficient to establish snapshot identity")
	t.Logf("B0=%s F1=%s R1=%s F2=%s R2=%s actualRollbackTarget=%s", base.Id, deployment.Id, rollbackDeployment.Id, retriedForward.DeploymentId, secondRollback.Id, secondRollback.RollbackToId.Must())
	require.Equal(t, base.Id, secondRollback.RollbackToId.Must(), "R2 must restore the exact base B0 captured before this operation's FIRST forward, not equivalent R1")
}

func verifyExternalRollbackCannotImplicitlyRetry(t *testing.T, fixture externalRollbackFixture) {
	t.Helper()
	ctx := fixture.ctx
	store := fixture.store
	rollbackDeployment := fixture.rollbackDeployment
	adapter := fixture.adapter
	actorID := fixture.actorID
	orgID := fixture.orgID
	pluginID := fixture.pluginID
	rollbackRequest := fixture.rollbackRequest
	environmentUUID := fixture.environmentUUID
	projectID := fixture.projectID
	projectUUID := fixture.projectUUID
	envID := fixture.envID
	runnerID := fixture.runnerID
	material := fixture.material
	t.Run("own failed rollback head requires explicit retry design", func(t *testing.T) {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback()) }()
		_, err = store.UpdateDeploymentStatusAndOutputs(ctx, tx, rollbackDeployment.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed})
		require.NoError(t, err)
		_, err = adapter.createExternalRollback(ctx, tx, actorID, orgID, pluginID, rollbackRequest, platformorchestratorcp.Environment{Uuid: environmentUUID, ProjectId: projectID, ProjectUuid: &projectUUID, Id: envID, RunnerId: ref.Ref(runnerID)}, material.Recipient)
		require.ErrorContains(t, err, "no longer the current head", "this narrow forward recovery must not silently authorize retry through another Deployment head")
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM deployments WHERE mode='rollback'").Scan(&count))
		require.Equal(t, 1, count)
	})
}

func verifyExternalAdmissionRevocationAndAssertionBinding(t *testing.T, fixture externalRollbackFixture) {
	t.Helper()
	grantActive, checks := fixture.grantActive, fixture.checks
	do, bodyBytes, assertion := fixture.do, fixture.bodyBytes, fixture.assertion
	dispatchID := fixture.claims.RequestID
	grantActive.Store(false)
	revoked := do(bodyBytes, assertion, dispatchID)
	require.Equal(t, http.StatusForbidden, revoked.Code, revoked.Body.String())
	require.EqualValues(t, 7, checks.Load())

	tampered := append([]byte(nil), bodyBytes...)
	tampered = bytes.Replace(tampered, []byte(`"fence":7`), []byte(`"fence":8`), 1)
	invalid := do(tampered, assertion, dispatchID)
	require.Equal(t, http.StatusUnauthorized, invalid.Code, invalid.Body.String())
	require.EqualValues(t, 7, checks.Load(), "invalid assertion must fail before Core callbacks")
}

func externalDeploymentTestDatabase(t *testing.T, ctx context.Context, connection string) model.Databaser {
	t.Helper()
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "external_http_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), fmt.Sprintf("%s search_path=%s", connection, schema))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

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
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratorcpv2 "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
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
			testExternalDeploymentHTTPAdapterRollback(t, status)
		})
	}
}

func testExternalDeploymentHTTPAdapterRollback(t *testing.T, forwardStatus model.DeploymentStatus) {
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
			var check platformorchestratorcpv2.PluginDispatchCheck
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
			_ = json.NewEncoder(response).Encode(platformorchestratorcpv2.PluginDispatchAuthorization{
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

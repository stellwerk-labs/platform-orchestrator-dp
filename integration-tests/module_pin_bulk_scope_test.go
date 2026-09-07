package integrationtests

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	iam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	dp "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

type pinScopeGrant struct {
	Scope       string
	Permissions []string
}

// Register real identities and real custom roles. No authorization store is
// mocked, and no write_all/read_all grant is used to mask the tested boundaries.
func mustPinScopePrincipal(t *testing.T, orgID string, grants ...pinScopeGrant) uuid.UUID {
	t.Helper()
	publicIAM, internalIAM := MustIamClient(t), MustInternalIamClient(t)
	actor := MustRegisterUser(t, publicIAM, MustGenerateTestUserToken(t))
	for _, grant := range grants {
		var role struct {
			ID uuid.UUID `json:"id"`
		}
		pinJSONAt(t, os.Getenv("INTERNAL_IAM_URL"), uuid.Nil, http.MethodPost, "/orgs/"+orgID+"/roles",
			map[string]any{"display_name": "Pin acceptance " + uuid.NewString(), "permissions": grant.Permissions}, "", http.StatusCreated, &role)
		membership, err := internalIAM.InternalCreateOrgMembershipWithResponse(t.Context(), orgID, iam.InternalCreateOrgMembershipJSONRequestBody{
			UserId: actor, Subject: role.ID.String(), SubjectType: iam.SubjectTypeRole, Scope: ref.Ref(grant.Scope),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, membership.StatusCode(), string(membership.Body))
		checks := make([]iam.ResourcePermissionCheck, 0, len(grant.Permissions))
		for _, permission := range grant.Permissions {
			checks = append(checks, iam.ResourcePermissionCheck{Resource: grant.Scope, Permission: permission})
		}
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			allowed, err := internalIAM.InternalAuthorizeWithResponse(t.Context(), iam.InternalAuthorizeBody{UserId: actor, Checks: checks})
			if assert.NoError(c, err) {
				assert.Equal(c, http.StatusNoContent, allowed.StatusCode(), string(allowed.Body))
			}
		}, 10*time.Second, 100*time.Millisecond)
	}
	return actor
}

func pinActorJSON(t *testing.T, actor uuid.UUID, method, path string, body any, key string, status int, result any) {
	t.Helper()
	pinJSONAt(t, os.Getenv("SERVER_URL"), actor, method, path, body, key, status, result)
}

func pinJSONAt(t *testing.T, baseURL string, actor uuid.UUID, method, path string, body any, key string, status int, result any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	requireLocalPinTestURL(t, baseURL)
	request, err := http.NewRequestWithContext(t.Context(), method, baseURL, bytes.NewReader(encoded)) // #nosec G704 -- Test URL is restricted to HTTP(S) loopback; response-derived identities cannot change its host.
	require.NoError(t, err)
	setPinRequestPath(t, request, path)
	if actor == uuid.Nil {
		actor = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	}
	if key == "" {
		key = uuid.NewString()
	}
	request.Header.Set("From", actor.String())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response, err := pinTestHTTPClient().Do(request) // #nosec G704 -- Validated loopback host, relative-only API path, and redirects disabled.
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	encoded, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, status, response.StatusCode, string(encoded))
	if result != nil {
		require.NoError(t, json.Unmarshal(encoded, result))
	}
}

func requireLocalPinTestURL(t *testing.T, raw string) {
	t.Helper()
	require.NoError(t, validatePinTestURL(raw))
}

func validatePinTestURL(raw string) error {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return fmt.Errorf("Pin acceptance requires an HTTP(S) server")
	}
	host := endpoint.Hostname()
	if host != "localhost" && !strings.HasSuffix(host, ".localhost") && !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("Pin acceptance requires a loopback integration server")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return fmt.Errorf("Pin acceptance server must not contain credentials, paths, queries or fragments")
	}
	return nil
}

func pinTestHTTPClient() *http.Client {
	client := *testHttpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

func setPinRequestPath(t *testing.T, request *http.Request, path string) {
	t.Helper()
	endpoint, err := parsePinRequestPath(path)
	require.NoError(t, err)
	// Response identities may select a relative API path, never the server host.
	request.URL.Path, request.URL.RawPath, request.URL.RawQuery = endpoint.Path, endpoint.RawPath, endpoint.RawQuery
}

func parsePinRequestPath(path string) (*url.URL, error) {
	endpoint, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, err
	}
	if endpoint.IsAbs() || endpoint.Host != "" || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") {
		return nil, fmt.Errorf("Pin acceptance endpoint must be a server-relative API path")
	}
	return endpoint, nil
}

func TestPinAcceptanceTransportGuards(t *testing.T) {
	for _, raw := range []string{"http://localhost:28080", "http://candidate.localhost:28081", "https://127.0.0.1", "http://[::1]:28083/"} {
		require.NoError(t, validatePinTestURL(raw))
	}
	for _, raw := range []string{"https://external.example", "http://localhost.external.example", "ftp://localhost", "http://user:password@localhost", "http://localhost/path", "http://localhost?target=elsewhere", "http://localhost#fragment"} {
		require.Error(t, validatePinTestURL(raw), raw)
	}
	for _, path := range []string{"/orgs/test/module-version-pins", "/orgs/test/memberships?userId=example"} {
		_, err := parsePinRequestPath(path)
		require.NoError(t, err)
	}
	for _, path := range []string{"https://external.example/api", "//external.example/api", "relative", "http://localhost/api"} {
		_, err := parsePinRequestPath(path)
		require.Error(t, err, path)
	}
	require.ErrorIs(t, pinTestHTTPClient().CheckRedirect(&http.Request{}, nil), http.ErrUseLastResponse)
}

type pinRecord struct {
	ID                uuid.UUID `json:"id"`
	EnvironmentUUID   uuid.UUID `json:"environment_uuid"`
	ModuleUUID        uuid.UUID `json:"module_uuid"`
	VersionUUID       uuid.UUID `json:"version_uuid"`
	CreatedBy         uuid.UUID `json:"created_by"`
	ActivationEventID uuid.UUID `json:"activation_event_id"`
	ResourceVersion   int64     `json:"resource_version"`
	Status            string    `json:"status"`
}

type pinPreview struct {
	Eligible    bool   `json:"eligible"`
	Fingerprint string `json:"fingerprint"`
	Items       []struct {
		EnvironmentUUID uuid.UUID `json:"environment_uuid"`
		EnvironmentID   string    `json:"environment_id"`
		EnvironmentType string    `json:"environment_type"`
		ProjectUUID     uuid.UUID `json:"project_uuid"`
		ProjectID       string    `json:"project_id"`
		Production      bool      `json:"production"`
		VersionUUID     uuid.UUID `json:"version_uuid"`
		Eligible        bool      `json:"eligible"`
		Problem         string    `json:"problem"`
	} `json:"items"`
}

type pinBulkResult struct {
	OperationID uuid.UUID   `json:"operation_id"`
	Pins        []pinRecord `json:"pins"`
}

func pinDatabaseSnapshot(t *testing.T, database *sql.DB, orgID string) string {
	t.Helper()
	var result string
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT jsonb_build_object(
		'pins', (SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.id), '[]'::jsonb) FROM environment_module_version_pins p WHERE p.org_id=$1),
		'events', (SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY e.sequence), '[]'::jsonb) FROM environment_module_version_pin_events e JOIN environment_module_version_pins p ON p.id=e.pin_id WHERE p.org_id=$1),
		'commands', (SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.command_scope,c.idempotency_key), '[]'::jsonb) FROM module_core_commands c WHERE c.org_id=$1)
	)`, orgID).Scan(&result))
	return result
}

func requireConcurrentPinBulkReplay(t *testing.T, actor uuid.UUID, path string, body any, key string, expected pinBulkResult) {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	baseURL := os.Getenv("SERVER_URL")
	requireLocalPinTestURL(t, baseURL)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL, nil) // #nosec G704 -- Same validated loopback transport boundary as pinJSONAt; no remote URL is accepted.
	require.NoError(t, err)
	setPinRequestPath(t, request, path)
	request.Header.Set("From", actor.String())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	type outcome struct {
		status int
		body   []byte
		err    error
	}
	const callers = 4
	results := make(chan outcome, callers)
	for range callers {
		go func() {
			call := request.Clone(request.Context())
			call.Body = io.NopCloser(bytes.NewReader(encoded))
			response, err := pinTestHTTPClient().Do(call) // #nosec G704 -- Clone preserves the validated loopback host/path; this client refuses redirects.
			if err != nil {
				results <- outcome{err: err}
				return
			}
			body, err := io.ReadAll(response.Body)
			if closeErr := response.Body.Close(); err == nil {
				err = closeErr
			}
			results <- outcome{status: response.StatusCode, body: body, err: err}
		}()
	}
	for range callers {
		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, http.StatusOK, result.status, string(result.body))
		var actual pinBulkResult
		require.NoError(t, json.Unmarshal(result.body, &actual))
		require.Equal(t, expected, actual)
	}
}

func TestBulkPinsUseFrozenDeployedVersionsAndRealScopedAuthority(t *testing.T) {
	control, data := MustControlPlaneClient(t), MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	projectA := MustCreateProject(t, control, orgID, "north")
	projectB := MustCreateProject(t, control, orgID, "south")
	envType := MustCreateEnvType(t, control, orgID, "development")
	MustCreateRunnerWithRule(t, control, orgID, "bulk-pin-runner", "", "", nil)
	envA := MustCreateEnv(t, control, orgID, envType.Id, projectA.Id, "development")
	envB := MustCreateEnv(t, control, orgID, envType.Id, projectB.Id, "development")
	rt, err := control.CreateResourceTypeWithResponse(t.Context(), orgID, cp.ResourceTypeCreateBody{
		Id: "bulk-value", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"value": map[string]interface{}{"type": "string"}}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rt.StatusCode(), string(rt.Body))
	const moduleID = "bulk-module"
	const source = `variable "value" { type = string }
resource "terraform_data" "value" { input = var.value }
output "value" { value = terraform_data.value.output }`
	created, err := createManagedModuleWithResponse(t, control, orgID, cp.ModuleCreateBody{
		Id: moduleID, ResourceType: rt.JSON201.Id, ModuleSource: "inline", ModuleSourceCode: ref.Ref(source), ModuleInputs: map[string]interface{}{"value": "first"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
	rule, err := control.CreateModuleRuleInOrgWithResponse(t.Context(), orgID, cp.RuleCreateBody{ModuleId: moduleID})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	identity, recipient := GetAgeIdentityAndRecipient(t)
	manifest := dp.DeploymentManifest{Workloads: map[string]dp.DeploymentManifestWorkload{
		"application": {Resources: map[string]dp.DeploymentManifestResource{"value": {Type: rt.JSON201.Id}}, Outputs: map[string]string{"VALUE": "${resources.value.outputs.value}"}},
	}}
	deploy := func(env *cp.Environment, expected string) {
		response, err := data.CreateDeploymentWithResponse(t.Context(), orgID, &dp.CreateDeploymentParams{}, dp.DeploymentCreateBody{
			ProjectId: env.ProjectId, EnvId: env.Id, Mode: dp.DeploymentCreateBodyModeDeploy, Manifest: &manifest, EncryptedOutputsRecipient: ref.Ref(recipient),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
		completed := MustWaitForDeploymentComplete(t, data, orgID, response.JSON201.Id)
		require.Equal(t, "succeeded", completed.Status, completed.StatusMessage)
		outputs, err := data.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgID, completed.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, outputs.StatusCode())
		require.JSONEq(t, `{"application":{"VALUE":"`+expected+`"}}`, decryptOutputs(t, identity, []byte(outputs.JSON200.Raw)))
	}
	deploy(envA, "first")
	updated, err := updateManagedModuleWithResponse(t, control, orgID, moduleID, cp.ModuleUpdateBody{
		ModuleSource: ref.Ref("inline"), ModuleSourceCode: ref.Ref(source), ModuleInputs: ref.Ref(map[string]interface{}{"value": "second"}),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updated.StatusCode(), string(updated.Body))
	deploy(envB, "second")
	root := "/orgs/" + orgID
	var detail struct {
		Version struct {
			UUID       uuid.UUID `json:"uuid"`
			ModuleUUID uuid.UUID `json:"module_uuid"`
		} `json:"version"`
	}
	pinActorJSON(t, uuid.Nil, http.MethodGet, root+"/modules/"+moduleID+"/versions/1.0.0", nil, "", http.StatusOK, &detail)
	moduleUUID, oldVersion := detail.Version.ModuleUUID, detail.Version.UUID
	pinActorJSON(t, uuid.Nil, http.MethodGet, root+"/modules/"+moduleID+"/versions/1.0.1", nil, "", http.StatusOK, &detail)
	newVersion := detail.Version.UUID
	corePermissions := []string{"module.version.pin", "module.version.unpin", "module.version.pin-note"}
	creator := mustPinScopePrincipal(t, orgID,
		pinScopeGrant{Scope: "project:" + projectA.Uuid.String(), Permissions: corePermissions},
		pinScopeGrant{Scope: "env:" + envB.Uuid.String(), Permissions: corePermissions})
	reviewer := mustPinScopePrincipal(t, orgID,
		pinScopeGrant{Scope: "project:" + projectA.Uuid.String(), Permissions: corePermissions},
		pinScopeGrant{Scope: "env:" + envB.Uuid.String(), Permissions: []string{"module.version.pin-override"}})
	database, err := sql.Open("postgres", os.Getenv("CP_DB_CONNECTION_STRING"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	selection := []uuid.UUID{envB.Uuid, envA.Uuid}
	preview := func(actor uuid.UUID, action string) pinPreview {
		var result pinPreview
		pinActorJSON(t, actor, http.MethodPost, root+"/module-version-pins/bulk-preview",
			map[string]any{"module_uuid": moduleUUID, "environment_uuids": selection, "action": action}, "", http.StatusOK, &result)
		return result
	}
	bulk := func(actor uuid.UUID, action string, plan pinPreview, key string, status int) pinBulkResult {
		var result pinBulkResult
		pinActorJSON(t, actor, http.MethodPost, root+"/module-version-pins/bulk",
			map[string]any{"module_uuid": moduleUUID, "environment_uuids": selection, "action": action,
				"preview_fingerprint": plan.Fingerprint, "reason": "Freeze the reviewed project snapshot"}, key, status, &result)
		return result
	}
	before := pinDatabaseSnapshot(t, database, orgID)
	deniedPreview := preview(reviewer, "pin")
	require.False(t, deniedPreview.Eligible)
	require.Len(t, deniedPreview.Items, 2)
	for _, item := range deniedPreview.Items {
		require.Equal(t, item.EnvironmentUUID == envA.Uuid, item.Eligible)
		if !item.Eligible {
			require.Empty(t, item.EnvironmentID, "a denied preview must not disclose the Environment name")
			require.Empty(t, item.EnvironmentType)
			require.Empty(t, item.ProjectID)
			require.Equal(t, uuid.Nil, item.ProjectUUID)
			require.Equal(t, uuid.Nil, item.VersionUUID)
			require.False(t, item.Production)
		}
	}
	bulk(reviewer, "pin", deniedPreview, "partial-permission-denied", http.StatusConflict)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	eligible := preview(creator, "pin")
	require.True(t, eligible.Eligible)
	versions := map[uuid.UUID]uuid.UUID{}
	for _, item := range eligible.Items {
		versions[item.EnvironmentUUID] = item.VersionUUID
	}
	require.Equal(t, map[uuid.UUID]uuid.UUID{envA.Uuid: oldVersion, envB.Uuid: newVersion}, versions)
	var individual pinRecord
	pinActorJSON(t, creator, http.MethodPost, root+"/module-version-pins", map[string]any{
		"project_uuid": projectA.Uuid, "environment_uuid": envA.Uuid, "module_uuid": moduleUUID,
		"version_uuid": oldVersion, "reason": "Pin the already-active Deprecated version"}, "", http.StatusCreated, &individual)
	before = pinDatabaseSnapshot(t, database, orgID)
	bulk(creator, "pin", eligible, "stale-preview", http.StatusConflict)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	pinActorJSON(t, creator, http.MethodPost, root+"/module-version-pins/"+individual.ID.String()+"/actions/unpin",
		map[string]any{"expected_resource_version": individual.ResourceVersion, "reason": "Prepare the reviewed bulk operation"}, "", http.StatusOK, nil)
	eligible = preview(creator, "pin")
	createdPins := bulk(creator, "pin", eligible, "frozen-bulk-pin", http.StatusOK)
	require.Len(t, createdPins.Pins, 2)
	pins := map[uuid.UUID]pinRecord{}
	for _, pin := range createdPins.Pins {
		require.Equal(t, creator, pin.CreatedBy)
		require.Equal(t, versions[pin.EnvironmentUUID], pin.VersionUUID)
		pins[pin.EnvironmentUUID] = pin
	}
	before = pinDatabaseSnapshot(t, database, orgID)
	replayed := bulk(creator, "pin", eligible, "frozen-bulk-pin", http.StatusOK)
	require.Equal(t, createdPins, replayed)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	requireConcurrentPinBulkReplay(t, creator, root+"/module-version-pins/bulk", map[string]any{
		"module_uuid": moduleUUID, "environment_uuids": selection, "action": "pin",
		"preview_fingerprint": eligible.Fingerprint, "reason": "Freeze the reviewed project snapshot"}, "frozen-bulk-pin", createdPins)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	bulk(reviewer, "pin", eligible, "frozen-bulk-pin", http.StatusForbidden)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))

	later := MustCreateEnv(t, control, orgID, envType.Id, projectA.Id, "later")
	deploy(later, "second")
	var laterPins []pinRecord
	pinActorJSON(t, uuid.Nil, http.MethodGet, root+"/module-version-pins?environment_uuid="+later.Uuid.String(), nil, "", http.StatusOK, &laterPins)
	require.Empty(t, laterPins, "new Environments must not inherit a Project snapshot Pin")
	before = pinDatabaseSnapshot(t, database, orgID)
	unpinDenied := preview(reviewer, "unpin")
	require.False(t, unpinDenied.Eligible)
	bulk(reviewer, "unpin", unpinDenied, "partial-unpin-denied", http.StatusConflict)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	validUnpinPreview := preview(creator, "unpin")
	require.True(t, validUnpinPreview.Eligible)
	beforeNote := pins[envA.Uuid]
	pinActorJSON(t, reviewer, http.MethodPost, root+"/module-version-pins/"+beforeNote.ID.String()+"/notes",
		map[string]any{"note": "Another scoped engineer reviewed this protection"}, "", http.StatusCreated, nil)
	var afterNote pinRecord
	pinActorJSON(t, uuid.Nil, http.MethodGet, root+"/module-version-pins/"+beforeNote.ID.String(), nil, "", http.StatusOK, &afterNote)
	require.Equal(t, beforeNote, afterNote)
	require.Equal(t, validUnpinPreview, preview(creator, "unpin"), "an append-only note must not invalidate an already reviewed Pin command")
	before = pinDatabaseSnapshot(t, database, orgID)
	pinB := pins[envB.Uuid]
	pinActorJSON(t, reviewer, http.MethodPost, root+"/module-version-pins/"+pinB.ID.String()+"/notes",
		map[string]any{"note": "Not authorized for South"}, "", http.StatusForbidden, nil)
	pinActorJSON(t, reviewer, http.MethodPost, root+"/module-version-pins/"+pinB.ID.String()+"/actions/unpin",
		map[string]any{"expected_resource_version": pinB.ResourceVersion, "reason": "Override is not Unpin"}, "", http.StatusForbidden, nil)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	pinActorJSON(t, reviewer, http.MethodPost, root+"/module-version-pins/"+beforeNote.ID.String()+"/actions/unpin",
		map[string]any{"expected_resource_version": beforeNote.ResourceVersion, "reason": "Scope-authorized removal by another creator"}, "", http.StatusOK, nil)
	before = pinDatabaseSnapshot(t, database, orgID)
	bulk(creator, "unpin", validUnpinPreview, "stale-unpin-preview", http.StatusConflict)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	var events []struct {
		Actor             uuid.UUID `json:"actor"`
		EventType         string    `json:"event_type"`
		Reason            string    `json:"reason"`
		ActivationEventID uuid.UUID `json:"activation_event_id"`
	}
	pinActorJSON(t, uuid.Nil, http.MethodGet, root+"/module-version-pins/"+beforeNote.ID.String()+"/events", nil, "", http.StatusOK, &events)
	require.Len(t, events, 3)
	require.Equal(t, creator, events[0].Actor)
	require.Equal(t, "Freeze the reviewed project snapshot", events[0].Reason)
	require.Equal(t, reviewer, events[1].Actor)
	require.Equal(t, "note_added", events[1].EventType)
	require.Equal(t, reviewer, events[2].Actor)
	require.Equal(t, "Scope-authorized removal by another creator", events[2].Reason)
	for _, event := range events {
		require.Equal(t, beforeNote.ActivationEventID, event.ActivationEventID)
	}
	pinActorJSON(t, reviewer, http.MethodPost, root+"/module-version-pins/"+beforeNote.ID.String()+"/notes",
		map[string]any{"note": "Removed protection cannot accept new notes"}, "", http.StatusConflict, nil)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	pinActorJSON(t, creator, http.MethodPost, root+"/module-version-pins/"+pinB.ID.String()+"/actions/unpin",
		map[string]any{"expected_resource_version": pinB.ResourceVersion, "reason": "Finish the exact snapshot"}, "", http.StatusOK, nil)
	eligible = preview(creator, "pin")
	bulk(creator, "pin", eligible, "second-bulk-pin", http.StatusOK)
	eligible = preview(creator, "unpin")
	removed := bulk(creator, "unpin", eligible, "complete-bulk-unpin", http.StatusOK)
	require.Len(t, removed.Pins, 2)
	for _, pin := range removed.Pins {
		require.Equal(t, "removed", pin.Status)
	}
	before = pinDatabaseSnapshot(t, database, orgID)
	require.Equal(t, removed, bulk(creator, "unpin", eligible, "complete-bulk-unpin", http.StatusOK))
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))

	// Revocation is performed through real IAM, not a test-local permission map.
	var memberships struct {
		Items []struct {
			ID    uuid.UUID `json:"id"`
			Scope string    `json:"scope"`
		} `json:"items"`
	}
	iamURL := os.Getenv("INTERNAL_IAM_URL")
	pinJSONAt(t, iamURL, uuid.Nil, http.MethodGet, root+"/memberships?userId="+creator.String(), nil, "", http.StatusOK, &memberships)
	var removedMembership bool
	for _, membership := range memberships.Items {
		if membership.Scope == "env:"+envB.Uuid.String() {
			pinJSONAt(t, iamURL, uuid.Nil, http.MethodDelete, root+"/memberships/"+membership.ID.String(), nil, "", http.StatusNoContent, nil)
			removedMembership = true
		}
	}
	require.True(t, removedMembership)
	internalIAM := MustInternalIamClient(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		allowed, err := internalIAM.InternalAuthorizeWithResponse(t.Context(), iam.InternalAuthorizeBody{UserId: creator,
			Checks: []iam.ResourcePermissionCheck{{Resource: "env:" + envB.Uuid.String(), Permission: "module.version.unpin"}}})
		if assert.NoError(c, err) {
			assert.Equal(c, http.StatusForbidden, allowed.StatusCode(), string(allowed.Body))
		}
	}, 70*time.Second, 100*time.Millisecond)
	bulk(creator, "unpin", eligible, "complete-bulk-unpin", http.StatusForbidden)
	require.JSONEq(t, before, pinDatabaseSnapshot(t, database, orgID))
	deploymentDatabase, err := sql.Open("postgres", os.Getenv("DB_CONNECTION_STRING"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, deploymentDatabase.Close()) })
	var deploymentCount int
	require.NoError(t, deploymentDatabase.QueryRowContext(t.Context(), `SELECT count(*) FROM deployments d
		JOIN deployment_environments e ON e.id=d.de_id WHERE e.org_id=$1`, orgID).Scan(&deploymentCount))
	require.Equal(t, 3, deploymentCount, "Pin, note and Unpin commands must not trigger infrastructure execution")
}

package integrationtests

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genclient"
)

const legacyProviderFixtureSHA256 = "a67a52b88190092a0e1004cbcfb8c7c4f03e8f04d7b47bb80c6384cd7f280745"

type legacyGraphState struct {
	SecretUID string `json:"secret_uid"`
	SHA256    string `json:"sha256"`
	Lineage   string `json:"lineage"`
	Serial    int64  `json:"serial"`
}

type legacyGraphUpgradeCase struct {
	OrgID           string           `json:"org_id"`
	ProjectID       string           `json:"project_id"`
	EnvironmentID   string           `json:"environment_id"`
	EnvironmentUUID uuid.UUID        `json:"environment_uuid"`
	BaseDeployment  uuid.UUID        `json:"base_deployment"`
	Namespace       string           `json:"namespace"`
	OutputIdentity  string           `json:"output_identity"`
	HistoricalGraph string           `json:"historical_graph_sha256"`
	HistoricalRow   string           `json:"historical_other_columns_sha256"`
	State           legacyGraphState `json:"state"`
}

// The default run is the normal real-server regression. The explicit private
// phases let an operator keep one owned DB/environment across a real old-to-new
// service upgrade. The synthetic old-wire fixture is not a historical dataset.
func TestLegacyProviderSubstitutionsExistingEnvironment(t *testing.T) {
	phase := os.Getenv("LEGACY_GRAPH_UPGRADE_PHASE")
	require.Contains(t, []string{"", "seed", "old-reader", "upgraded"}, phase)
	var fixture legacyGraphUpgradeCase
	if phase == "" || phase == "seed" {
		fixture = seedLegacyProviderGraph(t)
		if phase == "seed" {
			root := legacyGraphCaseRoot(t)
			raw, err := json.Marshal(fixture)
			require.NoError(t, err)
			file, err := root.OpenFile("case.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			require.NoError(t, err, "preserve the first owned upgrade case")
			_, err = file.Write(raw)
			require.NoError(t, err)
			require.NoError(t, file.Sync())
			require.NoError(t, file.Close())
			return
		}
	} else {
		root := legacyGraphCaseRoot(t)
		info, err := root.Lstat("case.json")
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
		require.EqualValues(t, 0o600, info.Mode().Perm())
		raw, err := root.ReadFile("case.json")
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &fixture))
	}
	assertLegacyGraphHistory(t, fixture)
	if phase == "old-reader" {
		client := MustDataPlaneClient(t)
		response, err := client.CreateDeploymentWithResponse(t.Context(), fixture.OrgID, &serverclient.CreateDeploymentParams{}, legacyGraphDeploymentBody(fixture, true, nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, response.StatusCode(), string(response.Body))
		// The normal HTTP boundary redacts internal errors; the private upgrade
		// rehearsal separately binds the old server log to the Gob failure.
		var failure struct {
			Code    string `json:"error"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(response.Body, &failure))
		require.Equal(t, "HTTP-500", failure.Code)
		require.Equal(t, "Unexpected error", failure.Message)
		var count int
		require.NoError(t, MustDatabaseConn(t).QueryRowContext(t.Context(), `SELECT count(*) FROM deployments WHERE de_id=$1`, fixture.EnvironmentUUID).Scan(&count))
		require.Equal(t, 1, count, "old decoding fails before a Deployment is admitted")
		require.Equal(t, fixture.State, readLegacyGraphState(t, fixture))
		assertLegacyGraphHistory(t, fixture)
		return
	}

	client := MustDataPlaneClient(t)
	plan := runLegacyGraphDeployment(t, fixture, true, nil)
	require.Equal(t, fixture.State, readLegacyGraphState(t, fixture), "plan-only must preserve exact state bytes, UID, lineage and serial")
	assertLegacyGraphHistory(t, fixture)
	first := runLegacyGraphDeployment(t, fixture, false, nil)
	assertCanonicalProviderGraph(t, fixture, first.Id)
	read, err := client.ListDeploymentResourceNodesWithResponse(t.Context(), fixture.OrgID, first.Id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, read.StatusCode(), string(read.Body))
	require.NotEmpty(t, read.JSON200.Items)
	second := runLegacyGraphDeployment(t, fixture, false, nil)
	assertCanonicalProviderGraph(t, fixture, second.Id)
	rollback := runLegacyGraphDeployment(t, fixture, false, &fixture.BaseDeployment)
	assertCanonicalProviderGraph(t, fixture, rollback.Id)
	require.NotEqual(t, fixture.BaseDeployment, rollback.Id)
	require.NotEqual(t, plan.Id, first.Id)
	require.NotEqual(t, first.Id, second.Id)
	after := readLegacyGraphState(t, fixture)
	require.Equal(t, fixture.State.SecretUID, after.SecretUID)
	require.Equal(t, fixture.State.Lineage, after.Lineage)
	require.GreaterOrEqual(t, after.Serial, fixture.State.Serial)
	assertLegacyGraphHistory(t, fixture)
}

func legacyGraphCaseRoot(t *testing.T) *os.Root {
	t.Helper()
	directory := os.Getenv("LEGACY_GRAPH_UPGRADE_CASE_DIRECTORY")
	require.True(t, filepath.IsAbs(directory), "staged upgrade requires an explicit private case directory")
	root, err := os.OpenRoot(directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	parent, err := root.Stat(".")
	require.NoError(t, err)
	require.True(t, parent.IsDir())
	require.EqualValues(t, 0, parent.Mode().Perm()&0o077, "case contains its synthetic output decryption key")
	return root
}

func seedLegacyProviderGraph(t *testing.T) legacyGraphUpgradeCase {
	t.Helper()
	cp := MustControlPlaneClient(t)
	org := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cp, org, "legacy-provider-graph")
	envType := MustCreateEnvType(t, cp, org, "development")
	environment := MustCreateEnv(t, cp, org, envType.Id, project.Id, "retained")
	namespace := os.Getenv("LEGACY_GRAPH_RUNNER_NAMESPACE")
	if namespace == "" {
		namespace = "platform-orchestrator-runner"
	}
	require.Empty(t, validation.IsDNS1123Label(namespace))
	createLegacyGraphRunner(t, cp, org, project.Id, envType.Id, namespace)
	provider, err := cp.CreateModuleProviderWithResponse(t.Context(), org, platformorchestratorcp.ModuleProviderCreateBody{
		ProviderType: "faulty", Id: "legacy", Source: "astromechza/faulty", VersionConstraint: "= 0.1.0",
		Configuration: map[string]interface{}{"required_boolean": "${var.legacy_provider_flag}"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, provider.StatusCode(), string(provider.Body))
	resourceType, err := cp.CreateResourceTypeWithResponse(t.Context(), org, platformorchestratorcp.ResourceTypeCreateBody{
		Id: "legacy-provider-value", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"value": map[string]interface{}{"type": "string"}}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resourceType.StatusCode(), string(resourceType.Body))
	module, err := createManagedModuleWithResponse(t, cp, org, platformorchestratorcp.ModuleCreateBody{
		Id: "legacy-provider-module", ResourceType: resourceType.JSON201.Id, ModuleSource: "inline",
		ProviderMapping: map[string]string{"faulty": "faulty.legacy"},
		ModuleSourceCode: ref.Ref(`terraform {
  required_providers { faulty = { source = "astromechza/faulty" } }
}
resource "faulty_example" "value" { required_boolean = true }
resource "terraform_data" "witness" { input = "retained-legacy-graph" }
output "value" { value = terraform_data.witness.output }
output "platform_orchestrator_metadata" { value = {} }
`),
	}, "value")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, module.StatusCode(), string(module.Body))
	rule, err := cp.CreateModuleRuleInOrgWithResponse(t.Context(), org, platformorchestratorcp.RuleCreateBody{ModuleId: module.JSON201.Id})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	identity, _ := GetAgeIdentityAndRecipient(t)
	fixture := legacyGraphUpgradeCase{OrgID: org, ProjectID: project.Id, EnvironmentID: environment.Id, EnvironmentUUID: environment.Uuid, Namespace: namespace, OutputIdentity: identity.String()}
	base := runLegacyGraphDeployment(t, fixture, false, nil)
	fixture.BaseDeployment = base.Id
	fixture.State = readLegacyGraphState(t, fixture)
	blob, err := os.ReadFile("../internal/graphs/testdata/legacy-provider-subs/historical-tfvar-integration.gob")
	require.NoError(t, err)
	require.Equal(t, legacyProviderFixtureSHA256, legacyGraphDigest(blob))
	require.Contains(t, string(blob), "*canyon_graph.TfVarPlaceholder")
	db := MustDatabaseConn(t)
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var original []byte
	require.NoError(
		t,
		tx.QueryRowContext(t.Context(), `SELECT d.graph FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE e.org_id=$1 AND e.project_id=$2 AND e.env_id=$3 AND e.id=$4 AND d.id=$5 AND d.status='succeeded' AND d.mode='deploy' FOR UPDATE OF d`, org, project.Id, environment.Id, environment.Uuid, base.Id).
			Scan(&original),
	)
	var graph interface{}
	require.NoError(t, json.Unmarshal(original, &graph))
	changed := replaceLegacyProviderBlob(t, graph, base64.StdEncoding.EncodeToString(blob))
	require.Equal(t, 1, changed, "replace only the owned module's provider map, never Gob type-name bytes")
	patched, err := json.Marshal(graph)
	require.NoError(t, err)
	result, err := tx.ExecContext(t.Context(), `UPDATE deployments SET graph=$1 WHERE id=$2 AND de_id=$3 AND graph=$4 AND status='succeeded' AND mode='deploy'`, patched, base.Id, environment.Uuid, original)
	require.NoError(t, err)
	count, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, tx.Commit())
	fixture.HistoricalGraph, fixture.HistoricalRow = readLegacyGraphHistory(t, fixture)
	t.Logf("owned synthetic legacy case org=%s env=%s deployment=%s fixture=%s", org, environment.Uuid, base.Id, legacyProviderFixtureSHA256)
	return fixture
}

func createLegacyGraphRunner(t *testing.T, cp platformorchestratorcp.ClientWithResponsesInterface, org, project, envType, namespace string) {
	t.Helper()
	raw, err := os.ReadFile("runner_config.json")
	require.NoError(t, err)
	var cluster platformorchestratorcp.K8sRunnerK8sCluster
	require.NoError(t, json.Unmarshal(raw, &cluster))
	spec := map[string]interface{}{"containers": []map[string]interface{}{{"name": "main", "env": []map[string]interface{}{{"name": "TF_VAR_legacy_provider_flag", "value": "true"}}}}}
	if address := os.Getenv("LEGACY_GRAPH_RUNNER_GATEWAY_IP"); address != "" {
		require.NotNil(t, net.ParseIP(address), "private callback alias must identify the owned gateway container")
		spec["hostAliases"] = []map[string]interface{}{{"ip": address, "hostnames": []string{"runner-gateway"}}}
	}
	configuration := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, configuration.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{Cluster: cluster, Job: platformorchestratorcp.K8sRunnerJobConfig{
		Namespace: namespace, ServiceAccount: "platform-orchestrator-runner", PodTemplate: &map[string]interface{}{"spec": spec},
	}}))
	storage := new(platformorchestratorcp.StateStorageConfiguration)
	require.NoError(t, storage.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: namespace}))
	created, err := cp.CreateRunnerWithResponse(t.Context(), org, platformorchestratorcp.RunnerCreateBody{Id: "legacy-provider-runner", RunnerConfiguration: *configuration, StateStorageConfiguration: *storage})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
	rule, err := cp.CreateRunnerRuleInOrgWithResponse(t.Context(), org, platformorchestratorcp.CreateRunnerRuleInOrgJSONRequestBody{ProjectId: &project, EnvTypeId: &envType, RunnerId: created.JSON201.Id})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
}

func legacyGraphDeploymentBody(f legacyGraphUpgradeCase, plan bool, rollback *uuid.UUID) serverclient.DeploymentCreateBody {
	identity, err := age.ParseX25519Identity(f.OutputIdentity)
	if err != nil {
		panic(err)
	}
	body := serverclient.DeploymentCreateBody{ProjectId: f.ProjectID, EnvId: f.EnvironmentID, Mode: serverclient.DeploymentCreateBodyModeDeploy, PlanOnly: &plan, EncryptedOutputsRecipient: ref.Ref(identity.Recipient().String())}
	if rollback != nil {
		body.Mode, body.RollbackToDeploymentId = serverclient.DeploymentCreateBodyModeRollback, rollback
	} else {
		body.Manifest = &serverclient.DeploymentManifest{
			Workloads: map[string]serverclient.DeploymentManifestWorkload{"application": {Resources: map[string]serverclient.DeploymentManifestResource{"value": {Type: "legacy-provider-value"}}, Outputs: map[string]string{"VALUE": "${resources.value.outputs.value}"}}},
		}
	}
	return body
}

func runLegacyGraphDeployment(t *testing.T, f legacyGraphUpgradeCase, plan bool, rollback *uuid.UUID) *serverclient.Deployment {
	t.Helper()
	client := MustDataPlaneClient(t)
	response, err := client.CreateDeploymentWithResponse(t.Context(), f.OrgID, &serverclient.CreateDeploymentParams{}, legacyGraphDeploymentBody(f, plan, rollback))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
	completed := MustWaitForDeploymentComplete(t, client, f.OrgID, response.JSON201.Id)
	require.Equal(t, "succeeded", completed.Status, completed.StatusMessage)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		job, err := MustCreateK8sClient(t).BatchV1().Jobs(f.Namespace).Get(t.Context(), completed.Id.String(), metav1.GetOptions{})
		if assert.NoError(c, err) {
			assert.EqualValues(c, 1, job.Status.Succeeded)
		}
	}, time.Minute, 250*time.Millisecond, "actual Runner Job must complete before state fingerprints")
	if !plan {
		output, err := client.GetDeploymentEncryptedOutputsWithResponse(t.Context(), f.OrgID, completed.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, output.StatusCode(), string(output.Body))
		identity, err := age.ParseX25519Identity(f.OutputIdentity)
		require.NoError(t, err)
		require.JSONEq(t, `{"application":{"VALUE":"retained-legacy-graph"}}`, decryptOutputs(t, identity, []byte(output.JSON200.Raw)))
	}
	return completed
}

func replaceLegacyProviderBlob(t *testing.T, value interface{}, encoded string) int {
	t.Helper()
	changed := 0
	switch object := value.(type) {
	case map[string]interface{}:
		if object["definition_id"] == "legacy-provider-module" {
			_, exists := object["provider_subs_map"].(string)
			require.True(t, exists)
			object["provider_subs_map"] = encoded
			return 1
		}
		for _, child := range object {
			changed += replaceLegacyProviderBlob(t, child, encoded)
		}
	case []interface{}:
		for _, child := range object {
			changed += replaceLegacyProviderBlob(t, child, encoded)
		}
	}
	return changed
}

func legacyGraphDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func readLegacyGraphHistory(t *testing.T, f legacyGraphUpgradeCase) (string, string) {
	t.Helper()
	var graph, rest []byte
	require.NoError(
		t,
		MustDatabaseConn(
			t,
		).QueryRowContext(t.Context(), `SELECT d.graph,(to_jsonb(d)-'graph')::text FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND d.de_id=$2 AND e.org_id=$3 AND e.project_id=$4 AND e.env_id=$5`, f.BaseDeployment, f.EnvironmentUUID, f.OrgID, f.ProjectID, f.EnvironmentID).
			Scan(&graph, &rest),
	)
	return legacyGraphDigest(graph), legacyGraphDigest(rest)
}

func assertLegacyGraphHistory(t *testing.T, f legacyGraphUpgradeCase) {
	t.Helper()
	graph, row := readLegacyGraphHistory(t, f)
	require.Equal(t, f.HistoricalGraph, graph, "old graph bytes must never be rewritten")
	require.Equal(t, f.HistoricalRow, row, "other historical Deployment columns must never be rewritten")
	environment, err := MustControlPlaneClient(t).GetEnvironmentWithResponse(t.Context(), f.OrgID, f.ProjectID, f.EnvironmentID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, environment.StatusCode(), string(environment.Body))
	require.Equal(t, f.EnvironmentUUID, environment.JSON200.Uuid)
}

func assertCanonicalProviderGraph(t *testing.T, f legacyGraphUpgradeCase, deployment uuid.UUID) {
	t.Helper()
	var raw []byte
	require.NoError(t, MustDatabaseConn(t).QueryRowContext(t.Context(), `SELECT graph FROM deployments WHERE id=$1 AND de_id=$2`, deployment, f.EnvironmentUUID).Scan(&raw))
	var value interface{}
	require.NoError(t, json.Unmarshal(raw, &value))
	found := 0
	var walk func(interface{})
	walk = func(v interface{}) {
		switch node := v.(type) {
		case map[string]interface{}:
			if node["definition_id"] == "legacy-provider-module" {
				encoded, ok := node["provider_subs_map"].(string)
				require.True(t, ok)
				blob, err := base64.StdEncoding.DecodeString(encoded)
				require.NoError(t, err)
				require.NotContains(t, string(blob), "*canyon_graph.")
				require.Contains(t, string(blob), "*orchestrator_graph.TfVarPlaceholder")
				found++
			}
			for _, child := range node {
				walk(child)
			}
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(value)
	require.Equal(t, 1, found)
}

func readLegacyGraphState(t *testing.T, f legacyGraphUpgradeCase) legacyGraphState {
	t.Helper()
	secret, err := MustCreateK8sClient(t).CoreV1().Secrets(f.Namespace).Get(t.Context(), "tfstate-default-"+f.EnvironmentUUID.String(), metav1.GetOptions{})
	require.NoError(t, err)
	raw, exists := secret.Data["tfstate"]
	require.True(t, exists)
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	decoded, err := io.ReadAll(io.LimitReader(reader, 1024*1024+1))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.LessOrEqual(t, len(decoded), 1024*1024)
	var state struct {
		Lineage   string            `json:"lineage"`
		Serial    int64             `json:"serial"`
		Resources []json.RawMessage `json:"resources"`
	}
	require.NoError(t, json.Unmarshal(decoded, &state))
	require.NotEmpty(t, state.Lineage)
	require.NotEmpty(t, state.Resources, "real Terraform state must be nonempty")
	return legacyGraphState{SecretUID: string(secret.UID), SHA256: legacyGraphDigest(raw), Lineage: state.Lineage, Serial: state.Serial}
}

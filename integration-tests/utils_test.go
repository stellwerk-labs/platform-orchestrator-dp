package integrationtests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stellwerk-labs/golib/hpostgresconnect"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

var testHttpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(host, ".localhost") {
				address = net.JoinHostPort("127.0.0.1", port)
			}
			dialer := &net.Dialer{
				Timeout: 30 * time.Second,
			}
			return dialer.DialContext(ctx, network, address)
		},
	},
}

func MustControlPlaneClient(t *testing.T) platformorchestratorcp.ClientWithResponsesInterface {
	client, err := platformorchestratorcp.NewClientWithResponses(os.Getenv("SERVER_URL"), platformorchestratorcp.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is internal - MustInternalControlPlaneClient client required", req.URL.Path)
		}
		return nil
	}), platformorchestratorcp.WithHTTPClient(testHttpClient))
	require.NoError(t, err)
	return client
}

func MustInternalControlPlaneClient(t *testing.T) platformorchestratorcp.ClientWithResponsesInterface {
	client, err := platformorchestratorcp.NewClientWithResponses(os.Getenv("INTERNAL_CP_URL"), platformorchestratorcp.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if !strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is not internal - MustControlPlaneClient required", req.URL.Path)
		}
		return nil
	}))
	require.NoError(t, err)
	return client
}

func MustDataPlaneClient(t *testing.T) serverclient.ClientWithResponsesInterface {
	client, err := serverclient.NewClientWithResponses(os.Getenv("SERVER_URL"), serverclient.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is internal - MustInternalDataPlaneClient client required", req.URL.Path)
		}
		return nil
	}), serverclient.WithHTTPClient(testHttpClient))
	require.NoError(t, err)
	return client
}

func MustDataPlaneClientWithUserId(t *testing.T, userId string) serverclient.ClientWithResponsesInterface {
	client, err := serverclient.NewClientWithResponses(os.Getenv("SERVER_URL"), serverclient.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", userId)
		if strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is internal - MustInternalDataPlaneClient client required", req.URL.Path)
		}
		return nil
	}), serverclient.WithHTTPClient(testHttpClient))
	require.NoError(t, err)
	return client
}

func MustInternalDataPlaneClient(t *testing.T) serverclient.ClientWithResponsesInterface {
	client, err := serverclient.NewClientWithResponses(os.Getenv("INTERNAL_DP_URL"), serverclient.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if !strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is not internal - MustDataPlaneClient required", req.URL.Path)
		}
		return nil
	}))
	require.NoError(t, err)
	return client
}

func MustIamClient(t *testing.T) platformorchestratoriam.ClientWithResponsesInterface {
	client, err := platformorchestratoriam.NewClientWithResponses(os.Getenv("SERVER_URL"), platformorchestratoriam.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is internal - MustInternalControlPlaneClient client required", req.URL.Path)
		}
		return nil
	}), platformorchestratoriam.WithHTTPClient(testHttpClient))
	require.NoError(t, err)
	return client
}

func MustInternalIamClient(t *testing.T) platformorchestratoriam.ClientWithResponsesInterface {
	client, err := platformorchestratoriam.NewClientWithResponses(os.Getenv("INTERNAL_IAM_URL"), platformorchestratoriam.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("From", "ffffffff-ffff-ffff-ffff-ffffffffffff")
		if !strings.HasPrefix(req.URL.Path, "/internal") {
			return fmt.Errorf("path %s is not internal - MustIamClient required", req.URL.Path)
		}
		return nil
	}))
	require.NoError(t, err)
	return client
}

func MustGenerateTestUserToken(t *testing.T) string {
	t.Helper()
	ageRecipient, err := age.ParseX25519Recipient(os.Getenv("TEST_USER_IDENTITY_RECIPIENT"))
	require.NoError(t, err)
	buff := new(bytes.Buffer)
	bw := base64.NewEncoder(base64.StdEncoding, buff)
	aw, _ := age.Encrypt(bw, ageRecipient)
	_ = json.NewEncoder(aw).Encode(map[string]string{
		"ProviderId":  rand.Text(),
		"DisplayName": "bob.smith",
	})
	_ = aw.Close()
	_ = bw.Close()
	return buff.String()
}

func MustRegisterUser(t *testing.T, iamClient platformorchestratoriam.ClientWithResponsesInterface, tut string) uuid.UUID {
	t.Helper()
	r, err := iamClient.RegisterUserWithResponse(t.Context(), &platformorchestratoriam.RegisterUserParams{}, platformorchestratoriam.RegisterUserJSONRequestBody{
		Provider:      "testuser",
		ProviderToken: tut,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, r.StatusCode(), string(r.Body))
	return r.JSON202.Id
}

func MustCreateMembershipInOrg(t *testing.T, internalIamClient, iamClient platformorchestratoriam.ClientWithResponsesInterface, orgId, roleDisplayName, scope string, userId uuid.UUID) {
	t.Helper()

	roles, err := iamClient.ListRolesWithResponse(t.Context(), orgId, nil)
	require.NoError(t, err)
	var roleId uuid.UUID
	require.Equal(t, http.StatusOK, roles.StatusCode(), "unexpected: %s", string(roles.Body))
	for _, r := range roles.JSON200.Items {
		if r.DisplayName == roleDisplayName {
			roleId = r.Id
			break
		}
	}
	require.NotEmpty(t, roleId)

	res, err := internalIamClient.InternalCreateOrgMembershipWithResponse(t.Context(), orgId, platformorchestratoriam.InternalCreateOrgMembershipJSONRequestBody{
		UserId:      userId,
		Subject:     roleId.String(),
		SubjectType: platformorchestratoriam.SubjectTypeRole,
		Scope:       ref.Ref(scope),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode(), "unexpected: %s", string(res.Body))
}

func MustOidcProviderClient(t *testing.T) serverclient.ClientWithResponsesInterface {
	client, err := serverclient.NewClientWithResponses(os.Getenv("OIDC_SERVER_URL"), serverclient.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		return nil
	}), serverclient.WithHTTPClient(testHttpClient))
	require.NoError(t, err)
	return client
}

// MustDatabaseConn provides access to a raw database connection for the integration test.
func MustDatabaseConn(t *testing.T) *hpostgresconnect.Database {
	t.Log(os.Getenv("DB_CONNECTION_STRING"))
	inner, err := hpostgresconnect.InitDatabase(t.Context(), &hpostgresconnect.Config{
		Logger:  zaptest.NewLogger(t),
		ConnStr: os.Getenv("DB_CONNECTION_STRING"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, inner.Close())
	})
	return inner
}

// MustDatabaser provides access to a database model instance.
func MustDatabaser(t *testing.T) model.Databaser {
	inner, err := model.NewDatabaser(t.Context(), zaptest.NewLogger(t), os.Getenv("DB_CONNECTION_STRING"))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, inner.Close())
	})
	return inner
}

// MustNATSConn provides access to NATS during tests.
func MustNATSConn(t *testing.T) *nats.Conn {
	conn, err := nats.Connect(os.Getenv("NATS_URL"))
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	return conn
}

func MustCreateOrgId(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface) string {
	t.Helper()
	res, err := cpClient.CreateInternalOrganizationWithResponse(t.Context(), platformorchestratorcp.CreateInternalOrganizationJSONRequestBody{Id: ref.Ref(fmt.Sprintf("org-%s", strings.ToLower(rand.Text())))})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode())
	return res.JSON201.Id
}

func MustCreateProject(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId string, appId string) *platformorchestratorcp.Project {
	t.Helper()
	res, err := cpClient.CreateProjectWithResponse(t.Context(), orgId, platformorchestratorcp.CreateProjectJSONRequestBody{Id: appId})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode())
	return res.JSON201
}

func MustCreateEnvType(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId string, et string) *platformorchestratorcp.EnvironmentType {
	t.Helper()
	res, err := cpClient.CreateEnvironmentTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateEnvironmentTypeJSONRequestBody{Id: et})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode())
	return res.JSON201
}

func MustCreateEnv(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, et, projectId, env string) *platformorchestratorcp.Environment {
	t.Helper()
	res, err := cpClient.CreateEnvironmentWithResponse(t.Context(), orgId, projectId, platformorchestratorcp.CreateEnvironmentJSONRequestBody{EnvTypeId: et, Id: env})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	return res.JSON201
}

// magicNoOpRunnerId copied from create dep handler to specify a manually-advanced deployment
const magicNoOpRunnerId = "sunshine-weary-robin-runner"

func MustCreateFakeK8sDefaultRunner(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId string) *platformorchestratorcp.Runner {
	var rc = new(platformorchestratorcp.RunnerConfiguration)
	_ = rc.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		}})
	var ssc = new(platformorchestratorcp.StateStorageConfiguration)
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{
		Namespace: "default",
	})
	rn := registerRunner(t, cpClient, *rc, orgId, magicNoOpRunnerId)

	_, err := cpClient.CreateRunnerRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.CreateRunnerRuleInOrgJSONRequestBody{
		RunnerId: rn.Id,
	})
	require.NoError(t, err)

	return rn
}

func MustCreateRunnerWithRule(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, runnerId, projectId, envTypeId string, jobConfig *platformorchestratorcp.K8sRunnerJobConfig) *platformorchestratorcp.Runner {
	cfgJson, err := os.ReadFile("runner_config.json")
	require.NoError(t, err)
	var rnConfigk8sCluster platformorchestratorcp.K8sRunnerK8sCluster
	require.NoError(t, json.Unmarshal(cfgJson, &rnConfigk8sCluster))
	var cfg = new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: rnConfigk8sCluster,
		Job: ref.DerefOr(jobConfig, platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
			PodTemplate: &map[string]interface{}{
				"spec": map[string]interface{}{
					"volumes": []map[string]interface{}{
						{
							"name": "terraform-module-success",
							"configMap": map[string]interface{}{
								"name": "terraform-module-success",
							},
						},
						{
							"name": "terraform-module-failure",
							"configMap": map[string]interface{}{
								"name": "terraform-module-failure",
							},
						},
					},
					"containers": []map[string]interface{}{
						{
							"name": "main",
							"volumeMounts": []map[string]interface{}{
								{
									"name":      "terraform-module-success",
									"mountPath": "/mnt/modules/terraform-module-success",
									"readOnly":  true,
								},
								{
									"name":      "terraform-module-failure",
									"mountPath": "/mnt/modules/terraform-module-failure",
									"readOnly":  true,
								},
							},
						},
					},
				},
			},
		}),
	}))
	rn := registerRunner(t, cpClient, *cfg, orgId, runnerId)

	_, err = cpClient.CreateRunnerRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.CreateRunnerRuleInOrgJSONRequestBody{
		ProjectId: ref.Ref(projectId),
		EnvTypeId: ref.Ref(envTypeId),
		RunnerId:  rn.Id,
	})
	require.NoError(t, err)

	return rn
}

func MustCreateK8sClient(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	config, err := clientcmd.BuildConfigFromFlags("", "runner-cluster/kubeconfig.yaml")
	require.NoError(t, err)
	clientset, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	return clientset
}

func MustGetDeploymentTofu(t *testing.T, dpClient serverclient.ClientWithResponsesInterface, orgId string, deploymentId uuid.UUID) string {
	t.Helper()
	r, err := dpClient.GetDeploymentTfWithResponse(t.Context(), orgId, deploymentId)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
	return string(r.Body)
}

func MakeTofuDeterministic(t *testing.T, tofu string, envUuid uuid.UUID) string {
	t.Helper()

	tofu = strings.ReplaceAll(tofu, envUuid.String(), "<env-uuid>")

	inlineModuleSourcePattern := regexp.MustCompile(`source = "\./modules/([^/]+)/([^"]+)"`)
	for _, m := range inlineModuleSourcePattern.FindAllStringSubmatch(tofu, -1) {
		tofu = strings.ReplaceAll(tofu, m[0], fmt.Sprintf(`source = "./modules/%s/<module-version-uuid>"`, m[1]))
	}

	metadataPattern := regexp.MustCompile(`"([^"]+)" = (lookup\(module\.[^,]+, "platform_orchestrator_metadata", \{}\))`)
	for _, m := range metadataPattern.FindAllStringSubmatch(tofu, -1) {
		tofu = strings.ReplaceAll(tofu, m[0], fmt.Sprintf(`"<node-hash>" = %s`, m[2]))
	}

	return tofu
}

func MakeDiffDeterministic(diff serverclient.DeploymentDiff) {
	moduleSourcePattern := regexp.MustCompile(`@[0-9a-f-]{36}`)
	for i, change := range diff.Changes {
		change.Id = "<node-hash>"
		change.Summary = moduleSourcePattern.ReplaceAllString(change.Summary, "@<module-version-uuid>")
		diff.Changes[i] = change
	}
}

func MustCreateRemoteRunnerWithRule(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, runnerId, projectId, envTypeId string) (runner *platformorchestratorcp.Runner, privateKey ed25519.PrivateKey) {
	t.Helper()
	var publicKey ed25519.PublicKey
	var err error
	publicKey, privateKey, err = ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err, "failed to generate ed25519 key pair")

	derBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err, "failed to marshal public key to DER format")
	pem := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derBytes,
	})
	var rnConfig = new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, rnConfig.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
		Type: platformorchestratorcp.RunnerTypeKubernetesAgent,
		Key:  string(pem),
	}))

	runner = registerRunner(t, cpClient, *rnConfig, orgId, runnerId)

	_, err = cpClient.CreateRunnerRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.CreateRunnerRuleInOrgJSONRequestBody{
		ProjectId: ref.Ref(projectId),
		EnvTypeId: ref.Ref(envTypeId),
		RunnerId:  runner.Id,
	})
	require.NoError(t, err)

	return runner, privateKey
}

func registerRunner(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, rnConfig platformorchestratorcp.RunnerConfiguration, orgId, runnerId string) *platformorchestratorcp.Runner {
	var ssc = new(platformorchestratorcp.StateStorageConfiguration)
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{
		Namespace: "platform-orchestrator-runner",
	})
	res, err := cpClient.CreateRunnerWithResponse(t.Context(), orgId, platformorchestratorcp.CreateRunnerJSONRequestBody{
		Id:                        runnerId,
		Description:               ref.Ref("default runner"),
		RunnerConfiguration:       rnConfig,
		StateStorageConfiguration: *ssc,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	return res.JSON201
}

func GetAgeIdentityAndRecipient(t *testing.T) (*age.X25519Identity, string) {
	var id, err = age.GenerateX25519Identity()
	require.NoError(t, err)
	return id, id.Recipient().String()
}

func nodeHash(envUuid uuid.UUID, x string) string {
	h := sha256.New()
	_, _ = fmt.Fprint(h, envUuid.String(), " ", x)
	return hex.EncodeToString(h.Sum(nil))
}

func MustWaitForDeploymentComplete(t *testing.T, dpClient serverclient.ClientWithResponsesInterface, orgId string, deploymentId uuid.UUID) *serverclient.Deployment {
	t.Helper()
	var after *serverclient.Deployment
	require.EventuallyWithTf(t, func(collect *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), orgId, deploymentId, &serverclient.WaitForDeploymentCompleteParams{})
		assert.NoError(collect, err)
		assert.Equal(collect, http.StatusOK, res.StatusCode())
		after = res.JSON200
	}, time.Minute*3, time.Second, "deployment %s not complete", deploymentId)
	return after
}

func MustCreateResourceType(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, resType string) *platformorchestratorcp.ResourceType {
	t.Helper()
	res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: resType,
		OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	return res.JSON201
}

func MustCreateModuleAndRule(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId string, bod platformorchestratorcp.ModuleCreateBody) *platformorchestratorcp.Module {
	t.Helper()
	modRes, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, bod)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, modRes.StatusCode(), string(modRes.Body))
	ruleRes, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.CreateModuleRuleInOrgJSONRequestBody{
		ModuleId: modRes.JSON201.Id,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, ruleRes.StatusCode(), string(ruleRes.Body))
	return modRes.JSON201
}

func MustCreateModuleForEnv(t *testing.T, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, projectId, envId string) *platformorchestratorcp.Rule {
	t.Helper()
	modRes, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           "md-" + strings.ToLower(rand.Text()),
		ResourceType: MustCreateResourceType(t, cpClient, orgId, "rt-"+strings.ToLower(rand.Text())).Id,
		ModuleSource: "git::https://github.com/delca85/v2-module-sources//definitions/dummy-k8s-namespace",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, modRes.StatusCode(), string(modRes.Body))
	ruleRes, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.CreateModuleRuleInOrgJSONRequestBody{
		ModuleId:  modRes.JSON201.Id,
		ProjectId: ref.Ref(projectId),
		EnvId:     ref.Ref(envId),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, ruleRes.StatusCode(), string(ruleRes.Body))
	return ruleRes.JSON201
}

func MustRunDeploymentAndGetOutputs(t *testing.T, dpClient serverclient.ClientWithResponsesInterface, orgId string, project, env string, manifest serverclient.DeploymentManifest) map[string]interface{} {
	t.Helper()
	var dep serverclient.Deployment
	id, recipient := GetAgeIdentityAndRecipient(t)
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: project, EnvId: env,
			Mode:                      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:                  &manifest,
			EncryptedOutputsRecipient: ref.Ref(recipient),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		require.NoError(c, err)
		if assert.Equal(c, http.StatusOK, res.StatusCode()) {
			require.Equal(c, "succeeded", res.JSON200.Status, res.JSON200.StatusMessage)
		}
	}, 2*time.Minute, 2*time.Second, "deployment not completed after 30s")

	res, err := dpClient.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode())
	decryptedOutputs := decryptOutputs(t, id, []byte(res.JSON200.Raw))
	var out map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(decryptedOutputs), &out))
	return out
}

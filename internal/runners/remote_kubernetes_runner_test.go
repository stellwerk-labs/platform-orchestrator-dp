package runners

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	v1 "k8s.io/api/batch/v1"
)

// mockServiceDiscovery is a mock implementation of DNSResolver for testing
type mockServiceDiscovery struct {
	ips  []net.IP
	host string
}

func (m *mockServiceDiscovery) LookupIP() ([]net.IP, error) {
	return m.ips, nil
}

func (m *mockServiceDiscovery) GetPort() string {
	parsedURL, _ := url.Parse(m.host)
	return parsedURL.Port()
}

func createTestRemoteKubernetesRunner(t *testing.T, serverURL string) (*RemoteKubernetesRunner, *model.DeploymentSummary, platformorchestratorcp.InternalRunner) {
	return createTestRemoteKubernetesRunnerWithDNS(t, serverURL, nil, 0)
}

func createTestRemoteKubernetesRunnerWithDNS(t *testing.T, externalDataplaneUrl string, dnsResolver ServiceDiscovery, _ int) (*RemoteKubernetesRunner, *model.DeploymentSummary, platformorchestratorcp.InternalRunner) {
	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace: "test-namespace",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: *cfg,
	}

	runner := NewRemoteKubernetesRunner(
		externalDataplaneUrl,
		testRunnerImage,
		testTokenSalt,
		zaptest.NewLogger(t),
		internalRunner,
		deploymentSummary,
		"localhost",
		runnerLogsBucketSignedUrl,
		5*time.Second,
	)

	if dnsResolver != nil {
		runner.dnsResolver = dnsResolver
	}

	return runner, deploymentSummary, internalRunner
}

func TestRemoteKubernetesRunner_Start_Success(t *testing.T) {
	var receivedMessage map[string]interface{}
	failingMocKServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		assert.NoError(t, err)
		receivedMessage = message
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServer.Close()
	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{failingMocKServer.Listener.Addr().(*net.TCPAddr).IP, mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, deploymentSummary, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	err := runner.Start(context.Background())
	require.NoError(t, err)

	assert.NotNil(t, receivedMessage)
	assert.Equal(t, "create-job", receivedMessage["action"])
	assert.Equal(t, deploymentSummary.Id.String(), receivedMessage["job_id"])
	assert.Equal(t, "test-namespace", receivedMessage["namespace"])

	config, ok := receivedMessage["configuration"].(map[string]any)
	assert.True(t, ok)
	assert.NotNil(t, config)

	assert.Contains(t, config, "template")
	template, ok := config["template"].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, template, "spec")
}

func TestRemoteKubernetesRunner_Start_DoesNotIncludeMetadataKey(t *testing.T) {
	var receivedMessage map[string]interface{}
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewDecoder(r.Body).Decode(&receivedMessage)
		assert.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}
	runner, _, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	err := runner.Start(context.Background())
	require.NoError(t, err)

	config, _ := receivedMessage["configuration"].(map[string]any)
	template, _ := config["template"].(map[string]any)
	spec, _ := template["spec"].(map[string]any)
	containers, _ := spec["containers"].([]any)
	require.NotEmpty(t, containers)
	container, _ := containers[0].(map[string]any)
	envList, _ := container["env"].([]any)

	for _, e := range envList {
		entry, _ := e.(map[string]any)
		assert.NotEqual(t, "METADATA_KEY", entry["name"], "METADATA_KEY must not be sent to the remote kubernetes runner")
	}
}

func TestRemoteKubernetesRunner_Start_HTTPError(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, _, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to push message to all resolved IPs")
}

func TestRemoteKubernetesRunner_Start_InvalidJobConfiguration(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
	}

	cfg := platformorchestratorcp.RunnerConfiguration{}

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: cfg,
	}

	runner := NewRemoteKubernetesRunner(
		mockServer.URL,
		testRunnerImage,
		testTokenSalt,
		zaptest.NewLogger(t),
		internalRunner,
		deploymentSummary,
		"localhost",
		runnerLogsBucketSignedUrl,
		5*time.Second,
	)

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get job configuration")
}

func TestRemoteKubernetesRunner_IsRunning(t *testing.T) {
	runner, _, _ := createTestRemoteKubernetesRunner(t, testExternalDataplaneUrl)

	isRunning, err := runner.IsRunning(context.Background())
	require.NoError(t, err)
	assert.False(t, isRunning)
}

func TestRemoteKubernetesRunner_MessageFormat(t *testing.T) {
	var receivedMessage map[string]interface{}
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		assert.NoError(t, err)
		receivedMessage = message
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServer.Close()
	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, deploymentSummary, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	err := runner.Start(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "create-job", receivedMessage["action"])
	assert.Equal(t, deploymentSummary.Id.String(), receivedMessage["job_id"])
	assert.Equal(t, "test-namespace", receivedMessage["namespace"])

	config, _ := json.Marshal(receivedMessage["configuration"])
	var jobSpec v1.JobSpec
	require.NoError(t, json.Unmarshal(config, &jobSpec))

	assert.NotEmpty(t, jobSpec.Template)
	assert.NotEmpty(t, jobSpec.Parallelism)
	assert.NotEmpty(t, jobSpec.TTLSecondsAfterFinished)

	template := jobSpec.Template
	spec := template.Spec
	containers := spec.Containers
	assert.Len(t, containers, 1)

	container := containers[0]
	assert.Equal(t, testRunnerImage, container.Image)
	assert.Equal(t, "main", container.Name)

	env := container.Env
	envMap := make(map[string]string)
	for _, envVar := range env {
		envMap[envVar.Name] = envVar.Value
	}

	assert.Equal(t, testOrgId, envMap["ORG_ID"])
	assert.Equal(t, deploymentSummary.Id.String(), envMap["DEPLOYMENT_ID"])
	assert.Equal(t, mockServer.URL, envMap["PLATFORM_ORCHESTRATOR_BASE_URL"])
	assert.NotEmpty(t, envMap["TOKEN"])
}

func TestRemoteKubernetesRunner_CheckStatus_Success(t *testing.T) {
	var receivedMessage map[string]interface{}
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		assert.NoError(t, err)
		receivedMessage = message
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, deploymentSummary, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, status)
	assert.False(t, status.IsCompleted)
	assert.False(t, status.IsStuck)

	// Verify the message sent to remote runner
	assert.NotNil(t, receivedMessage)
	assert.Equal(t, "check-job-status", receivedMessage["action"])
	assert.Equal(t, deploymentSummary.Id.String(), receivedMessage["job_id"])
	assert.Equal(t, "test-namespace", receivedMessage["namespace"])
	assert.NotEmpty(t, receivedMessage["deployment_token"])
}

func TestRemoteKubernetesRunner_CheckStatus_HTTPError(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, _, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	status, err := runner.CheckStatus(context.Background())
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "failed to push message to all resolved IPs")
}

func TestRemoteKubernetesRunner_CheckStatus_MultipleServers(t *testing.T) {
	var receivedMessage map[string]interface{}

	// First server fails
	failingMockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failingMockServer.Close()

	// Second server succeeds
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		assert.NoError(t, err)
		receivedMessage = message
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{failingMockServer.Listener.Addr().(*net.TCPAddr).IP, mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, deploymentSummary, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, status)
	assert.False(t, status.IsCompleted)

	// Verify the message was received by the successful server
	assert.NotNil(t, receivedMessage)
	assert.Equal(t, "check-job-status", receivedMessage["action"])
	assert.Equal(t, deploymentSummary.Id.String(), receivedMessage["job_id"])
}

func TestRemoteKubernetesRunner_CheckStatus_DNSLookupFailure(t *testing.T) {
	failingDNS := &failingServiceDiscovery{
		err: assert.AnError,
	}

	runner, _, _ := createTestRemoteKubernetesRunner(t, "http://example.com")
	runner.dnsResolver = failingDNS

	status, err := runner.CheckStatus(context.Background())
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "failed to lookup service IPs")
}

func TestRemoteKubernetesRunner_CheckStatus_NoIPsFound(t *testing.T) {
	runner, _, _ := createTestRemoteKubernetesRunner(t, "http://example.com")

	runner.dnsResolver = &defaultServiceDiscovery{
		host: "non-existent-host-that-should-not-resolve.local",
		port: "8080",
	}

	status, err := runner.CheckStatus(context.Background())
	require.Error(t, err)
	assert.Nil(t, status)
	assert.True(t,
		err.Error() == "no IPs found for service" ||
			strings.Contains(err.Error(), "failed to lookup service IPs"))
}

// failingServiceDiscovery is a mock that always returns an error
type failingServiceDiscovery struct {
	err error
}

func (f *failingServiceDiscovery) LookupIP() ([]net.IP, error) {
	return nil, f.err
}

func (f *failingServiceDiscovery) GetPort() string {
	return "8080"
}

func TestRemoteKubernetesRunner_Start_KubernetesAgentNotReachable(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, _, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	// Update deployment creation time to be very recent (should trigger ErrKubernetesAgentNotReachable)
	runner.deploymentSummary.CreatedAt = time.Now()

	err := runner.Start(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKubernetesAgentNotReachableRetry)
}

func TestRemoteKubernetesRunner_CheckStatus_KubernetesAgentNotReachable(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	mockDNS := &mockServiceDiscovery{
		host: mockServer.URL,
		ips:  []net.IP{mockServer.Listener.Addr().(*net.TCPAddr).IP},
	}

	runner, _, _ := createTestRemoteKubernetesRunnerWithDNS(t, mockServer.URL, mockDNS, mockServer.Listener.Addr().(*net.TCPAddr).Port)

	// Update deployment creation time to be very recent (should trigger ErrKubernetesAgentNotReachable)
	runner.deploymentSummary.CreatedAt = time.Now()

	status, err := runner.CheckStatus(context.Background())
	require.Error(t, err)
	assert.Nil(t, status)
	require.ErrorIs(t, err, ErrKubernetesAgentNotReachableRetry)
}

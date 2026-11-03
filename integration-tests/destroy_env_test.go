package integrationtests

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/clientcmd"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genclient"
)

func TestDestroyEmptyEnv_delete_rules(t *testing.T) {
	t.Parallel()
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")
	moduleRuleId := MustCreateModuleForEnv(t, cpClient, orgId, projectId, env.Id).Id

	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{DeleteRules: ref.Ref(true)})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode())
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, time.Second*10, time.Second, "failed to wait for env to be deleted")

	r, err := cpClient.GetModuleRuleInOrgWithResponse(t.Context(), orgId, moduleRuleId)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))

	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_nominal(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")
	moduleRuleId := MustCreateModuleForEnv(t, cpClient, orgId, projectId, env.Id).Id

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{},
						Outputs:   map[string]string{"KEY": "VALUE"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithTf(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(c, err) && assert.Equal(c, http.StatusOK, res.StatusCode(), string(res.Body)) {
			dep = *res.JSON200
			assert.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)

	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode())
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")

	r, err := cpClient.GetModuleRuleInOrgWithResponse(t.Context(), orgId, moduleRuleId)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, r.StatusCode(), "got %s", string(r.Body))
	require.Equal(t, env.Id, *r.JSON200.EnvId)
	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_resume_after_fail(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{},
						Outputs:   map[string]string{"KEY": "VALUE"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(t, err) && assert.Equal(c, http.StatusOK, res.StatusCode()) {
			dep = *res.JSON200
			assert.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)

	// We don't actually have a way to fail a deployment right now! So we're simulating a direct failure by status update.
	{
		r, err := internalCpClient.InternalUpdateEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, platformorchestratorcp.EnvironmentInternalUpdateBody{Status: ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed), StatusMessage: ref.Ref("test")})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleteFailed, r.JSON200.Status)
	}

	// We should be able to resume deletion when it is in delete failed.
	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode(), string(r.Body))
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")
	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_force(t *testing.T) {
	for _, deleteRules := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_rules=%t", deleteRules), func(t *testing.T) {
			t.Parallel()
			testDestroyEnvForceWithLiveRunner(t, deleteRules)
		})
	}
}

func testDestroyEnvForceWithLiveRunner(t *testing.T, deleteRules bool) {
	t.Helper()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	marker := uuid.NewString()
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", &platformorchestratorcp.K8sRunnerJobConfig{
		Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner",
		PodTemplate: &map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{"force-delete-fixture": marker}}},
	})
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "thing",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := createManagedModuleWithResponse(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{Id: "thing-def", ResourceType: "thing",
			ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`
resource "terraform_data" "thing" {
  provisioner "local-exec" {
    command = "touch /tmp/force-delete-entered; while [ ! -f /tmp/force-delete-release ]; do sleep 0.1; done"
  }
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	var ruleID uuid.UUID
	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "thing-def", ProjectId: &projectId, EnvId: &env.Id})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		ruleID = res.JSON201.Id
	}

	var deployment serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		deployment = *res.JSON201
	}

	k8sClient := MustCreateK8sClient(t)
	const namespace = "platform-orchestrator-runner"
	var heldPod corev1.Pod
	var jobUID string
	barrierReleased := false
	t.Cleanup(func() {
		if !barrierReleased && heldPod.UID != "" {
			if err := execForceDeleteBarrier(t, heldPod, types.UID(jobUID), marker, true); err != nil {
				t.Logf("failed to release own fixture barrier during cleanup: %v", err)
			}
		}
	})
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		job, err := k8sClient.BatchV1().Jobs(namespace).Get(t.Context(), deployment.Id.String(), metav1.GetOptions{})
		if !assert.NoError(c, err) {
			return
		}
		assert.EqualValues(c, 1, job.Status.Active)
		pods, err := k8sClient.CoreV1().Pods(namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "force-delete-fixture=" + marker})
		if !assert.NoError(c, err) || !assert.Len(c, pods.Items, 1) {
			return
		}
		pod := pods.Items[0]
		assert.Equal(c, corev1.PodRunning, pod.Status.Phase)
		if !assert.NoError(c, execForceDeleteBarrier(t, pod, job.UID, marker, false)) {
			return
		}
		heldPod, jobUID = pod, string(job.UID)
	}, 45*time.Second, 250*time.Millisecond, "real Runner must enter the held provisioner before force deletion")
	require.NotEmpty(t, jobUID)
	t.Logf("force deletion held real Job %s UID=%s Pod=%s UID=%s", deployment.Id, jobUID, heldPod.Name, heldPod.UID)

	queued, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{IdempotencyKey: ref.Ref("force-delete-queued-witness")}, serverclient.CreateDeploymentJSONRequestBody{
		ProjectId: projectId, EnvId: env.Id, Mode: serverclient.DeploymentCreateBodyModeDeploy, WhenBusy: ref.Ref(serverclient.Queue),
		Manifest: &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{
			"sample": {Resources: map[string]serverclient.DeploymentManifestResource{"t": {Type: "thing"}}},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, queued.StatusCode(), string(queued.Body))
	require.NotNil(t, queued.JSON202)

	res, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{
		Force: ref.Ref(true), DeleteRules: &deleteRules,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, res.StatusCode(), string(res.Body))

	// Only deletion cleanup cancels this exact queued item with this reason.
	// Observe its durable progress before releasing actual Runner work.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		item, err := dpClient.GetDeploymentQueueItemWithResponse(t.Context(), orgId, queued.JSON202.Id)
		if !assert.NoError(c, err) || !assert.Equal(c, http.StatusOK, item.StatusCode()) {
			return
		}
		if !assert.NotNil(c, item.JSON200) {
			return
		}
		assert.Equal(c, serverclient.Cancelled, item.JSON200.State)
		if assert.NotNil(c, item.JSON200.ReasonCode) {
			assert.Equal(c, "environment_deleting", *item.JSON200.ReasonCode)
		}
		assert.Nil(c, item.JSON200.DeploymentId, "the waiting request never became another Deployment")
	}, 15*time.Second, 250*time.Millisecond, "deletion handler must consume canonical policy before the Runner barrier releases")

	// The barrier still keeps actual work live after cleanup made progress.
	// Force can neither forget ownership nor admit a second destroy.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		current, err := dpClient.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: &projectId, EnvId: &env.Id})
		if !assert.NoError(c, err) || !assert.Equal(c, http.StatusOK, current.StatusCode()) {
			return
		}
		if assert.Len(c, current.JSON200.Items, 1) {
			assert.Equal(c, deployment.Id, current.JSON200.Items[0].Id)
			assert.Equal(c, "executing", current.JSON200.Items[0].Status)
		}
		cpEnv, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		if assert.NoError(c, err) && assert.Equal(c, http.StatusOK, cpEnv.StatusCode()) {
			assert.Equal(c, platformorchestratorcp.EnvironmentStatusDeleting, cpEnv.JSON200.Status)
			assert.Equal(c, env.Uuid, cpEnv.JSON200.Uuid)
			if assert.NotNil(c, cpEnv.JSON200.DeletionPolicy) {
				assert.True(c, cpEnv.JSON200.DeletionPolicy.Force)
				assert.Equal(c, deleteRules, cpEnv.JSON200.DeletionPolicy.DeleteRules)
			}
		}
		job, err := k8sClient.BatchV1().Jobs(namespace).Get(t.Context(), deployment.Id.String(), metav1.GetOptions{})
		if assert.NoError(c, err) {
			assert.Equal(c, jobUID, string(job.UID))
			assert.EqualValues(c, 1, job.Status.Active)
		}
	}, 10*time.Second, 250*time.Millisecond)
	require.NoError(t, execForceDeleteBarrier(t, heldPod, types.UID(jobUID), marker, true))
	barrierReleased = true

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		job, err := k8sClient.BatchV1().Jobs(namespace).Get(t.Context(), deployment.Id.String(), metav1.GetOptions{})
		if assert.NoError(c, err) {
			assert.Equal(c, jobUID, string(job.UID))
			assert.EqualValues(c, 1, job.Status.Succeeded)
		}
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		if assert.NoError(c, err) {
			assert.Equal(c, http.StatusNotFound, r.StatusCode(), string(r.Body))
		}
	}, time.Minute, 250*time.Millisecond, "natural Runner completion must resume canonical force deletion")
	remaining, err := dpClient.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: &projectId, EnvId: &env.Id})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, remaining.StatusCode(), string(remaining.Body))
	require.NotNil(t, remaining.JSON200)
	require.Empty(t, remaining.JSON200.Items)
	pods, err := k8sClient.CoreV1().Pods(namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "force-delete-fixture=" + marker})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1, "force must not create a destroy Runner")
	require.Equal(t, heldPod.UID, pods.Items[0].UID)
	rule, err := cpClient.GetModuleRuleInOrgWithResponse(t.Context(), orgId, ruleID)
	require.NoError(t, err)
	if deleteRules {
		require.Equal(t, http.StatusNotFound, rule.StatusCode())
	} else {
		require.Equal(t, http.StatusOK, rule.StatusCode())
	}
}

// execForceDeleteBarrier changes only the test workload barrier in the exact
// original Pod. Completion still travels through the real Runner result path.
func execForceDeleteBarrier(t *testing.T, pod corev1.Pod, jobUID types.UID, marker string, release bool) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 3*time.Second)
	defer cancel()
	expectedContext := os.Getenv("RUNNER_TEST_KUBE_CONTEXT")
	if expectedContext == "" || len(validation.IsDNS1123Subdomain(expectedContext)) != 0 {
		return fmt.Errorf("explicit valid owned Runner test context is required")
	}
	config, err := clientcmd.LoadFromFile("runner-cluster/kubeconfig.yaml")
	if err != nil {
		return err
	}
	selected := config.Contexts[expectedContext]
	if config.CurrentContext != expectedContext || selected == nil || selected.Cluster != expectedContext || selected.AuthInfo != expectedContext {
		return fmt.Errorf("Runner fixture kubeconfig/context ownership mismatch")
	}
	if err := validateForceDeleteBarrierPod(pod, pod.UID, jobUID, marker); err != nil {
		return err
	}
	client := MustCreateK8sClient(t)
	current, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateForceDeleteBarrierPod(*current, pod.UID, jobUID, marker); err != nil {
		return err
	}
	var process *exec.Cmd
	if release {
		//nolint:gosec // G204: fixed owned kubeconfig/context; Pod/Job UIDs and marker validated; separate identifier argv and a literal command.
		process = exec.CommandContext(ctx, "kubectl", "--kubeconfig", "runner-cluster/kubeconfig.yaml", "--context", expectedContext, "exec", "--namespace", "platform-orchestrator-runner", pod.Name, "--container", "main", "--", "/bin/sh", "-c", "touch /tmp/force-delete-release")
	} else {
		//nolint:gosec // G204: fixed owned kubeconfig/context; Pod/Job UIDs and marker validated; separate identifier argv and a literal command.
		process = exec.CommandContext(ctx, "kubectl", "--kubeconfig", "runner-cluster/kubeconfig.yaml", "--context", expectedContext, "exec", "--namespace", "platform-orchestrator-runner", pod.Name, "--container", "main", "--", "/bin/sh", "-c", "test -f /tmp/force-delete-entered")
	}
	if err := process.Run(); err != nil {
		return err
	}
	current, err = client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	return validateForceDeleteBarrierPod(*current, pod.UID, jobUID, marker)
}

func validateForceDeleteBarrierPod(pod corev1.Pod, podUID, jobUID types.UID, marker string) error {
	if pod.Namespace != "platform-orchestrator-runner" || len(validation.IsDNS1123Subdomain(pod.Name)) != 0 {
		return fmt.Errorf("invalid owned Runner fixture Pod target")
	}
	if podUID == "" || jobUID == "" || pod.UID != podUID {
		return fmt.Errorf("Runner fixture Pod UID mismatch")
	}
	if _, err := uuid.Parse(marker); err != nil || pod.Labels["force-delete-fixture"] != marker {
		return fmt.Errorf("Runner fixture marker mismatch")
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.UID == jobUID && owner.Controller != nil && *owner.Controller && owner.Name == pod.Labels["job-name"] {
			return nil
		}
	}
	return fmt.Errorf("Runner fixture owning Job UID mismatch")
}

func TestForceDeleteBarrierPodValidation(t *testing.T) {
	marker := uuid.NewString()
	podUID, jobUID := types.UID(uuid.NewString()), types.UID(uuid.NewString())
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner-job-pod", Namespace: "platform-orchestrator-runner", UID: podUID,
		Labels:          map[string]string{"force-delete-fixture": marker, "job-name": "runner-job"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "runner-job", UID: jobUID, Controller: ref.Ref(true)}},
	}}
	require.NoError(t, validateForceDeleteBarrierPod(pod, podUID, jobUID, marker))
	for name, change := range map[string]func(*corev1.Pod){
		"option-shaped name": func(p *corev1.Pod) { p.Name = "--context=other" },
		"wrong namespace":    func(p *corev1.Pod) { p.Namespace = "default" },
		"changed Pod UID":    func(p *corev1.Pod) { p.UID = "replacement" },
		"changed Job UID":    func(p *corev1.Pod) { p.OwnerReferences[0].UID = "replacement" },
		"changed marker":     func(p *corev1.Pod) { p.Labels["force-delete-fixture"] = uuid.NewString() },
		"unowned Pod":        func(p *corev1.Pod) { p.OwnerReferences = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := pod.DeepCopy()
			change(changed)
			require.Error(t, validateForceDeleteBarrierPod(*changed, podUID, jobUID, marker))
		})
	}
}

package kubernetes

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
)

func GetJobSpec(ctx context.Context, jobConfig platformorchestratorcp.K8sRunnerJobConfig, runnerImage, metadataOutputKey string, d *model.DeploymentSummary, gatewayConfigurations ...runnercommon.GatewayConfiguration) (*v1.JobSpec, error) {
	containerName := RunnerContainerName
	jobTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyNever,
			ServiceAccountName: jobConfig.ServiceAccount,
			Containers: []corev1.Container{
				{
					Name: containerName,
					Resources: corev1.ResourceRequirements{
						Requests: map[corev1.ResourceName]resource.Quantity{
							"memory": resource.MustParse(DefaultMemoryRequest),
						},
					},
					Image:           runnerImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Args:            []string{"standard"},
					Env: slices.Collect(func(yield func(corev1.EnvVar) bool) {
						for k, v := range runnercommon.GenerateEnvVarsForRun(metadataOutputKey, d, gatewayConfigurations...) {
							yield(corev1.EnvVar{Name: k, Value: v})
						}
					}),
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: ref.Ref(false),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{
								"ALL",
							},
						},
						RunAsNonRoot: ref.Ref(true),
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      TofuDirVolumeName,
							MountPath: TofuDirMountPath,
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: TofuDirVolumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}

	if jobConfig.PodTemplate != nil {
		jsonTemplate, _ := json.Marshal(jobTemplate)
		var template map[string]interface{}
		_ = json.Unmarshal(jsonTemplate, &template)

		patchedTemplate, err := strategicpatch.StrategicMergeMapPatch(template, *jobConfig.PodTemplate, corev1.PodTemplateSpec{})
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply the supplied pod template to the original template")
		}

		var podTemplate corev1.PodTemplateSpec
		if err := runtime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(patchedTemplate, &podTemplate, true); err != nil {
			return nil, errors.Wrap(err, "failed to build pod template from the merged")
		}
		jobTemplate = podTemplate
	}

	if d.Mode == model.DeploymentModeObservation {
		if err := validateObservationJobTemplate(jobTemplate); err != nil {
			return nil, err
		}
	}
	return &v1.JobSpec{
		TTLSecondsAfterFinished: ref.Ref(int32(DefaultRunnerJobTTL.Seconds())),
		Parallelism:             ref.Ref(int32(1)),
		BackoffLimit:            ref.Ref(int32(0)),
		Template:                jobTemplate,
	}, nil
}

// Templates may customize an ordinary job. Observation's final transport must
// retain the known safe legacy mode and the exact new protocol marker.
func validateObservationJobTemplate(template corev1.PodTemplateSpec) error {
	found := false
	for _, container := range template.Spec.Containers {
		if container.Name != RunnerContainerName {
			continue
		}
		found = true
		if len(container.Command) != 0 || !slices.Equal(container.Args, []string{runnercommon.RunnerImageSubCommand}) {
			return errors.New("observation requires the standard Runner entrypoint")
		}
		counts := map[string]int{}
		expected := map[string]string{"MODE": string(model.DeploymentModeDeployPlan), runnercommon.ObservationProtocolVariable: runnercommon.ObservationProtocolV1}
		for _, env := range container.Env {
			if value, protected := expected[env.Name]; protected {
				counts[env.Name]++
				if env.ValueFrom != nil || env.Value != value {
					return errors.New("observation transport environment cannot be overridden by a pod template")
				}
			}
		}
		for name := range expected {
			if counts[name] != 1 {
				return errors.New("observation requires exactly one safe mode and protocol marker")
			}
		}
	}
	if !found {
		return errors.New("observation Runner container is missing")
	}
	return nil
}

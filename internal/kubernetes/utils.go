package kubernetes

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
)

func GetJobSpec(ctx context.Context, jobConfig platformorchestratorcp.K8sRunnerJobConfig, runnerImage, metadataOutputKey string, d *model.DeploymentSummary, natsConfigurations ...runnercommon.NATSConfiguration) (*v1.JobSpec, error) {
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
						for k, v := range runnercommon.GenerateEnvVarsForRun(metadataOutputKey, d, natsConfigurations...) {
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

	return &v1.JobSpec{
		TTLSecondsAfterFinished: ref.Ref(int32(DefaultRunnerJobTTL.Seconds())),
		Parallelism:             ref.Ref(int32(1)),
		BackoffLimit:            ref.Ref(int32(0)),
		Template:                jobTemplate,
	}, nil
}

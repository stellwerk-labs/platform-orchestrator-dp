package runners

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/mocks/mockecs"
)

type TemporaryAuthProviderFunc func(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error)

func (f TemporaryAuthProviderFunc) ExchangeAwsCredentials(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error) {
	return f(ctx, orgId, runnerId, region, auth)
}

func Test_full_nominal(t *testing.T) {
	t.Parallel()

	rc := new(platformorchestratorcp.RunnerConfiguration)
	_ = rc.FromServerlessEcsRunnerConfiguration(platformorchestratorcp.ServerlessEcsRunnerConfiguration{
		Job: platformorchestratorcp.ServerlessEcsRunnerJob{
			Region:            "us-east-1",
			Cluster:           "my-cluster",
			ExecutionRoleArn:  "arn:aws:iam::123456789012:role/aws-exec-role",
			TaskRoleArn:       ref.Ref("arn:aws:iam::123456789012:role/aws-task-role"),
			Subnets:           []string{"subnet-1"},
			SecurityGroups:    []string{"sg-1"},
			IsPublicIpEnabled: true,
			Environment:       map[string]string{"A": "B"},
			Secrets:           map[string]string{"B": "some-arn"},
		},
	})
	r := &ecsRunnerInstance{
		RunnerTokenSalt:      "salty",
		RunnerImage:          "my-image",
		ExternalDataplaneUrl: "some-url",
		RunnerLogsSignedUrl:  "some-signed-url",

		TemporaryAuthProvider: TemporaryAuthProviderFunc(func(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error) {
			return &aws.AWSCredentials{
				AccessKeyID:     "foo",
				SecretAccessKey: "bar",
				SessionToken:    "baz",
			}, nil
		}),
		Runner: platformorchestratorcp.InternalRunner{
			Id:                  "my-runner",
			RunnerConfiguration: *rc,
		},
		Deployment: &model.DeploymentSummary{
			Id:             uuid.Nil,
			OrgId:          "my-org",
			ProjectId:      "my-project",
			EnvId:          "my-env",
			Mode:           model.DeploymentModeDeployPlan,
			RunnerLogLevel: "info",
		},
	}

	t.Run("can start", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(gomock.Any(), &ecs.ListTaskDefinitionsInput{
			FamilyPrefix: ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			Status:       types.TaskDefinitionStatusActive,
		}).Return(&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(gomock.Any(), &ecs.ListTaskDefinitionsInput{
			FamilyPrefix: ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			Status:       types.TaskDefinitionStatusInactive,
		}).Return(&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RegisterTaskDefinition(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, input *ecs.RegisterTaskDefinitionInput, opts ...func(*ecs.Options)) (*ecs.RegisterTaskDefinitionOutput, error) {
			actualRaw, _ := json.Marshal(input)
			expectedRaw, _ := json.Marshal(ecs.RegisterTaskDefinitionInput{
				Cpu:                     ref.Ref(".5 vCPU"),
				Memory:                  ref.Ref("1 GB"),
				NetworkMode:             types.NetworkModeAwsvpc,
				Family:                  ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
				RequiresCompatibilities: []types.Compatibility{types.CompatibilityFargate},
				ExecutionRoleArn:        ref.Ref("arn:aws:iam::123456789012:role/aws-exec-role"),
				TaskRoleArn:             ref.Ref("arn:aws:iam::123456789012:role/aws-task-role"),
				ContainerDefinitions: []types.ContainerDefinition{{
					Name:    ref.Ref("main"),
					Image:   ref.Ref("my-image"),
					Command: []string{"standard"},
					Environment: []types.KeyValuePair{
						{Name: ref.Ref("ORG_ID"), Value: ref.Ref("my-org")},
						{Name: ref.Ref("DEPLOYMENT_ID"), Value: ref.Ref("00000000-0000-0000-0000-000000000000")},
						{Name: ref.Ref("MODE"), Value: ref.Ref("plan_only")},
						{Name: ref.Ref("TOKEN"), Value: ref.Ref("gTTvt7GCPbVpz5fEWouA0Qn6F1trRpac39AgbVXgiOQ")},
						{Name: ref.Ref("PLATFORM_ORCHESTRATOR_BASE_URL"), Value: ref.Ref("some-url")},
						{Name: ref.Ref("PLATFORM_ORCHESTRATOR_API_PREFIX"), Value: ref.Ref("some-url")},
						{Name: ref.Ref("LOGS_URL"), Value: ref.Ref("some-signed-url")},
						{Name: ref.Ref("LOG_LEVEL"), Value: ref.Ref("info")},
						{Name: ref.Ref("A"), Value: ref.Ref("B")},
					},
					Secrets: []types.Secret{{Name: ref.Ref("B"), ValueFrom: ref.Ref("some-arn")}},
				}},
				Tags: []types.Tag{
					{Key: ref.Ref("ManagedBy"), Value: ref.Ref("platform-orchestrator-platform-orchestrator")},
					{Key: ref.Ref("PlatformOrchestratorOrgId"), Value: ref.Ref("my-org")},
					{Key: ref.Ref("PlatformOrchestratorProjectId"), Value: ref.Ref("my-project")},
					{Key: ref.Ref("PlatformOrchestratorEnvId"), Value: ref.Ref("my-env")},
					{Key: ref.Ref("PlatformOrchestratorRunnerId"), Value: ref.Ref("my-runner")},
				},
			})
			assert.JSONEq(t, string(expectedRaw), string(actualRaw))
			return &ecs.RegisterTaskDefinitionOutput{
				TaskDefinition: &types.TaskDefinition{
					TaskDefinitionArn: ref.Ref("my-task-def-arn"),
				},
			}, nil
		})
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RunTask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, input *ecs.RunTaskInput, opts ...func(*ecs.Options)) (*ecs.RunTaskOutput, error) {
			actualRaw, _ := json.Marshal(input)              //nolint:gosec
			expectedRaw, _ := json.Marshal(ecs.RunTaskInput{ //nolint:gosec
				TaskDefinition: ref.Ref("my-task-def-arn"),
				ClientToken:    ref.Ref("00000000-0000-0000-0000-000000000000"),
				Cluster:        ref.Ref("my-cluster"),
				LaunchType:     types.LaunchTypeFargate,
				NetworkConfiguration: &types.NetworkConfiguration{
					AwsvpcConfiguration: &types.AwsVpcConfiguration{
						AssignPublicIp: types.AssignPublicIpEnabled,
						SecurityGroups: []string{"sg-1"},
						Subnets:        []string{"subnet-1"},
					},
				},
				Tags: []types.Tag{
					{Key: ref.Ref("ManagedBy"), Value: ref.Ref("platform-orchestrator-platform-orchestrator")},
					{Key: ref.Ref("PlatformOrchestratorOrgId"), Value: ref.Ref("my-org")},
					{Key: ref.Ref("PlatformOrchestratorProjectId"), Value: ref.Ref("my-project")},
					{Key: ref.Ref("PlatformOrchestratorEnvId"), Value: ref.Ref("my-env")},
					{Key: ref.Ref("PlatformOrchestratorRunnerId"), Value: ref.Ref("my-runner")},
					{Key: ref.Ref("PlatformOrchestratorDeploymentId"), Value: ref.Ref("00000000-0000-0000-0000-000000000000")},
				},
			})
			assert.JSONEq(t, string(expectedRaw), string(actualRaw))
			return &ecs.RunTaskOutput{
				Tasks: []types.Task{{
					TaskArn: ref.Ref("my-task-arn"),
				}},
			}, nil
		})
		require.NoError(t, r.Start(t.Context()))
	})

	t.Run("can start with cleanup", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(gomock.Any(), &ecs.ListTaskDefinitionsInput{
			FamilyPrefix: ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			Status:       types.TaskDefinitionStatusActive,
		}).Return(&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{
			"active-task-def-arn",
		}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DeregisterTaskDefinition(gomock.Any(), &ecs.DeregisterTaskDefinitionInput{TaskDefinition: ref.Ref("active-task-def-arn")}).
			Return(&ecs.DeregisterTaskDefinitionOutput{}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(gomock.Any(), &ecs.ListTaskDefinitionsInput{
			FamilyPrefix: ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			Status:       types.TaskDefinitionStatusInactive,
		}).Return(&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{
			"active-task-def-arn",
			"inactive-task-def-arn",
		}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DeleteTaskDefinitions(gomock.Any(), &ecs.DeleteTaskDefinitionsInput{TaskDefinitions: []string{
			"active-task-def-arn",
			"inactive-task-def-arn",
		}}).
			Return(&ecs.DeleteTaskDefinitionsOutput{}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RegisterTaskDefinition(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, input *ecs.RegisterTaskDefinitionInput, opts ...func(*ecs.Options)) (*ecs.RegisterTaskDefinitionOutput, error) {
			return &ecs.RegisterTaskDefinitionOutput{
				TaskDefinition: &types.TaskDefinition{
					TaskDefinitionArn: ref.Ref("my-task-def-arn"),
				},
			}, nil
		})
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RunTask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, input *ecs.RunTaskInput, opts ...func(*ecs.Options)) (*ecs.RunTaskOutput, error) {
			return &ecs.RunTaskOutput{
				Tasks: []types.Task{{
					TaskArn: ref.Ref("my-task-arn"),
				}},
			}, nil
		})
		require.NoError(t, r.Start(t.Context()))
	})

	t.Run("is running not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), gomock.Any()).Return(&ecs.ListTasksOutput{TaskArns: []string{}}, nil)
		b, err := r.IsRunning(t.Context())
		require.NoError(t, err)
		assert.False(t, b)
	})

	t.Run("is running", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusRunning,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), &ecs.DescribeTasksInput{
			Cluster: ref.Ref("my-cluster"),
			Tasks:   []string{"some-task-arn"},
			Include: []types.TaskField{types.TaskFieldTags},
		}).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusRunning)),
			LastStatus:    ref.Ref(string(types.DesiredStatusPending)),
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.Nil.String())}},
		}}}, nil)
		b, err := r.IsRunning(t.Context())
		require.NoError(t, err)
		assert.True(t, b)
	})

	t.Run("is running no tag", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), gomock.Any()).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), gomock.Any()).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusRunning)),
			LastStatus:    ref.Ref(string(types.DesiredStatusPending)),
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.New().String())}},
		}}}, nil)
		b, err := r.IsRunning(t.Context())
		require.NoError(t, err)
		assert.False(t, b)
	})

	t.Run("check status running", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusRunning,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), &ecs.DescribeTasksInput{
			Cluster: ref.Ref("my-cluster"),
			Tasks:   []string{"some-task-arn"},
			Include: []types.TaskField{types.TaskFieldTags},
		}).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusRunning)),
			LastStatus:    ref.Ref(string(types.DesiredStatusRunning)),
			StartedAt:     &time.Time{},
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.Nil.String())}},
		}}}, nil)
		s, err := r.CheckStatus(t.Context())
		require.NoError(t, err)
		assert.Equal(t, RunnerStatus{IsCompleted: false, IsStuck: false, Message: ""}, *s)
	})

	t.Run("check status not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusRunning,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusStopped,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{}}, nil)
		s, err := r.CheckStatus(t.Context())
		require.NoError(t, err)
		assert.Equal(t, RunnerStatus{IsCompleted: true, IsStuck: false, Message: "task not found"}, *s)
	})

	t.Run("check status stopped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusRunning,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), &ecs.ListTasksInput{
			Cluster:       ref.Ref("my-cluster"),
			Family:        ref.Ref("platform_orchestrator_env_my-org_00000000-0000-0000-0000-000000000000"),
			DesiredStatus: types.DesiredStatusStopped,
		}).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), &ecs.DescribeTasksInput{
			Cluster: ref.Ref("my-cluster"),
			Tasks:   []string{"some-task-arn"},
			Include: []types.TaskField{types.TaskFieldTags},
		}).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusStopped)),
			LastStatus:    ref.Ref(string(types.DesiredStatusStopped)),
			StartedAt:     &time.Time{},
			StoppedAt:     &time.Time{},
			StopCode:      types.TaskStopCodeEssentialContainerExited,
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.Nil.String())}},
		}}}, nil)
		s, err := r.CheckStatus(t.Context())
		require.NoError(t, err)
		assert.Equal(t, RunnerStatus{IsCompleted: true, IsStuck: false, Message: ""}, *s)
	})

	t.Run("check status stuck pending", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), gomock.Any()).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), gomock.Any()).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusRunning)),
			LastStatus:    ref.Ref(string(types.DesiredStatusPending)),
			CreatedAt:     &time.Time{},
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.Nil.String())}},
		}}}, nil)
		s, err := r.CheckStatus(t.Context())
		require.NoError(t, err)
		assert.Equal(t, RunnerStatus{IsCompleted: false, IsStuck: true, Message: "ECS runner task is stuck in status PENDING (see https://us-east-1.console.aws.amazon.com/ecs/v2/clusters/my-cluster/tasks/some-task-arn for details)"}, *s)
	})

	t.Run("check status stuck stopping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTasks(gomock.Any(), gomock.Any()).Return(&ecs.ListTasksOutput{TaskArns: []string{"some-task-arn"}}, nil)
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().DescribeTasks(gomock.Any(), gomock.Any()).Return(&ecs.DescribeTasksOutput{Tasks: []types.Task{{
			DesiredStatus: ref.Ref(string(types.DesiredStatusStopped)),
			LastStatus:    ref.Ref("STOPPING"),
			StoppingAt:    &time.Time{},
			TaskArn:       ref.Ref("some-task-arn"),
			Tags:          []types.Tag{{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(uuid.Nil.String())}},
		}}}, nil)
		s, err := r.CheckStatus(t.Context())
		require.NoError(t, err)
		assert.Equal(t, RunnerStatus{IsCompleted: false, IsStuck: true, Message: "ECS runner task is stuck in status STOPPING (see https://us-east-1.console.aws.amazon.com/ecs/v2/clusters/my-cluster/tasks/some-task-arn for details)"}, *s)
	})

	t.Run("uses custom image when provided", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Test scenario: Verify that when a custom image is specified in the job configuration,
		// it overrides the default runner image
		customImage := "custom-runner-image:latest"

		// Setup runner configuration with custom image specified
		rc := new(platformorchestratorcp.RunnerConfiguration)
		_ = rc.FromServerlessEcsRunnerConfiguration(platformorchestratorcp.ServerlessEcsRunnerConfiguration{
			Job: platformorchestratorcp.ServerlessEcsRunnerJob{
				Region:            "us-east-1",
				Cluster:           "my-cluster",
				ExecutionRoleArn:  "arn:aws:iam::123456789012:role/aws-exec-role",
				TaskRoleArn:       ref.Ref("arn:aws:iam::123456789012:role/aws-task-role"),
				Subnets:           []string{"subnet-1"},
				SecurityGroups:    []string{"sg-1"},
				IsPublicIpEnabled: true,
				Image:             &customImage, // This is the key setting we're testing
			},
		})

		// Create ecsRunnerInstance with default image that should be overridden
		r := &ecsRunnerInstance{
			RunnerTokenSalt:      "salty",
			RunnerImage:          "default-image", // This should be ignored when custom image is provided
			ExternalDataplaneUrl: "some-url",
			RunnerLogsSignedUrl:  "some-signed-url",
			TemporaryAuthProvider: TemporaryAuthProviderFunc(
				func(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error) {
					return &aws.AWSCredentials{
						AccessKeyID:     "foo",
						SecretAccessKey: "bar",
						SessionToken:    "baz",
					}, nil
				},
			),
			Runner: platformorchestratorcp.InternalRunner{
				Id:                  "my-runner",
				RunnerConfiguration: *rc,
			},
			Deployment: &model.DeploymentSummary{
				Id:             uuid.Nil,
				OrgId:          "my-org",
				ProjectId:      "my-project",
				EnvId:          "my-env",
				Mode:           model.DeploymentModeDeployPlan,
				RunnerLogLevel: "info",
			},
		}

		// Mock the ECS client to intercept AWS API calls
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)

		// Mock ListTaskDefinitions calls (called twice - once for active, once for inactive task definitions)
		// Returns empty list to simulate no existing task definitions, forcing creation of a new one
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(
			gomock.Any(),
			gomock.Any(),
		).Return(
			&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{}},
			nil,
		).Times(2)

		// Mock RegisterTaskDefinition call and verify the custom image is used
		// This is the core assertion: the task definition should contain our custom image
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RegisterTaskDefinition(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(
			func(ctx context.Context, input *ecs.RegisterTaskDefinitionInput, opts ...func(*ecs.Options)) (*ecs.RegisterTaskDefinitionOutput, error) {
				// Verify that the container definition uses the custom image, not the default
				assert.Equal(t, customImage, *input.ContainerDefinitions[0].Image)
				return &ecs.RegisterTaskDefinitionOutput{
					TaskDefinition: &types.TaskDefinition{
						TaskDefinitionArn: ref.Ref("my-task-def-arn"),
					},
				}, nil
			},
		)

		// Mock RunTask call without errors
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RunTask(
			gomock.Any(),
			gomock.Any(),
		).Return(
			&ecs.RunTaskOutput{Tasks: []types.Task{{TaskArn: ref.Ref("my-task-arn")}}},
			nil,
		)
		require.NoError(t, r.Start(t.Context()))
	})

	t.Run("uses default image when no custom image specified", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Test scenario: Verify that when no custom image is specified,
		// the default runner image is used
		defaultImage := "default-runner-image"

		// Setup runner configuration without custom image
		rc := new(platformorchestratorcp.RunnerConfiguration)
		_ = rc.FromServerlessEcsRunnerConfiguration(platformorchestratorcp.ServerlessEcsRunnerConfiguration{
			Job: platformorchestratorcp.ServerlessEcsRunnerJob{
				Region:           "us-east-1",
				Cluster:          "my-cluster",
				ExecutionRoleArn: "arn:aws:iam::123456789012:role/aws-exec-role",
			},
		})

		r := &ecsRunnerInstance{
			RunnerTokenSalt:      "salty",
			RunnerImage:          defaultImage, // This should be used when no custom image is provided
			ExternalDataplaneUrl: "some-url",
			RunnerLogsSignedUrl:  "some-signed-url",
			TemporaryAuthProvider: TemporaryAuthProviderFunc(func(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error) {
				return &aws.AWSCredentials{
					AccessKeyID:     "foo",
					SecretAccessKey: "bar",
					SessionToken:    "baz",
				}, nil
			}),
			Runner: platformorchestratorcp.InternalRunner{
				Id:                  "my-runner",
				RunnerConfiguration: *rc,
			},
			Deployment: &model.DeploymentSummary{
				Id:             uuid.Nil,
				OrgId:          "my-org",
				ProjectId:      "my-project",
				EnvId:          "my-env",
				Mode:           model.DeploymentModeDeployPlan,
				RunnerLogLevel: "info",
			},
		}

		// Mock ECS client
		r.overrideClient = mockecs.NewMockecsClientSubset(ctrl)

		// Mock ListTaskDefinitions calls
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().ListTaskDefinitions(gomock.Any(), gomock.Any()).Return(&ecs.ListTaskDefinitionsOutput{TaskDefinitionArns: []string{}}, nil).Times(2)

		// Mock RegisterTaskDefinition and verify default image is used
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RegisterTaskDefinition(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(
			func(ctx context.Context, input *ecs.RegisterTaskDefinitionInput, opts ...func(*ecs.Options)) (*ecs.RegisterTaskDefinitionOutput, error) {
				// Verify that the container definition uses the default image
				assert.Equal(t, defaultImage, *input.ContainerDefinitions[0].Image)

				return &ecs.RegisterTaskDefinitionOutput{TaskDefinition: &types.TaskDefinition{TaskDefinitionArn: ref.Ref("task-def-arn")}}, nil
			},
		)

		// Mock RunTask call without errors
		r.overrideClient.(*mockecs.MockecsClientSubset).EXPECT().RunTask(
			gomock.Any(),
			gomock.Any(),
		).Return(
			&ecs.RunTaskOutput{Tasks: []types.Task{{TaskArn: ref.Ref("task-arn")}}},
			nil,
		)
		require.NoError(t, r.Start(context.Background()))
	})
}

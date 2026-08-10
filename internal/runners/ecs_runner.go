//go:generate go tool mockgen -destination=mocks/mockecs/ecs.go -package mockecs github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners ecsClientSubset
package runners

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
)

const (
	// ecsDefaultCpuUnits and ecsDefaultMemoryMegabytes need to be valid bin-packing combinations from
	// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#task_size.
	ecsDefaultCpuUnits        = ".5 vCPU"
	ecsDefaultMemoryMegabytes = "1 GB"

	ecsOrgIdTag          = "PlatformOrchestratorOrgId"
	ecsProjectIdTag      = "PlatformOrchestratorProjectId"
	ecsEnvIdTag          = "PlatformOrchestratorEnvId"
	ecsRunnerIdTag       = "PlatformOrchestratorRunnerId"
	ecsDeploymentIdTag   = "PlatformOrchestratorDeploymentId"
	ecsManagedByTag      = "ManagedBy"
	ecsManagedByTagValue = "platform-orchestrator-platform-orchestrator"

	ecsSchedulingTimeout = time.Minute * 2
	// ecsDeleteStaleTaskDefinitionLimit sets the limit of task definitions to delete before each deployment. We limit this
	// for two reasons: (1) because we want to cap the network IO performed. (2) because the delete task defs API enforces
	// a limit of 10.
	ecsDeleteStaleTaskDefinitionLimit = 10
)

var (
	ecsTaskRunningStatus = map[string]bool{
		string(types.DesiredStatusPending): true,
		string(types.DesiredStatusRunning): true,
		// these don't have constants in the ecs api spec because they are not desired statuses
		"PROVISIONING": true,
		"ACTIVATING":   true,
	}
	ecsAssignPublicIp = map[bool]types.AssignPublicIp{
		true:  types.AssignPublicIpEnabled,
		false: types.AssignPublicIpDisabled,
	}
)

type ecsRunnerInstance struct {
	Runner                platformorchestratorcp.InternalRunner
	TemporaryAuthProvider AwsTemporaryAuthProvider
	RunnerImage           string
	MetadataOutputKey     string
	Deployment            *model.DeploymentSummary
	NATSConfiguration     runnercommon.NATSConfiguration

	overrideClient ecsClientSubset
}

// The AwsTemporaryAuthProvider provides the auth exchange for aws credentials needed by the ecs runner.
// This is an interface for testing/mocking reasons.
type AwsTemporaryAuthProvider interface {
	ExchangeAwsCredentials(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error)
}

// mustRunnerConfiguration a helper method to convert the runner config into the typed config. This cannot fail since
// it has been validated in the upper layers already.
func (e *ecsRunnerInstance) mustRunnerConfiguration() platformorchestratorcp.ServerlessEcsRunnerConfiguration {
	c, _ := e.Runner.RunnerConfiguration.AsServerlessEcsRunnerConfiguration()
	return c
}

func (e *ecsRunnerInstance) getRunnerImage(runCfg platformorchestratorcp.ServerlessEcsRunnerConfiguration) string {
	if runCfg.Job.Image != nil && *runCfg.Job.Image != "" {
		return *runCfg.Job.Image
	}

	return e.RunnerImage
}

type ecsClientSubset interface {
	ecs.ListTasksAPIClient
	ecs.DescribeTasksAPIClient
	ecs.ListTaskDefinitionsAPIClient
	RegisterTaskDefinition(context.Context, *ecs.RegisterTaskDefinitionInput, ...func(*ecs.Options)) (*ecs.RegisterTaskDefinitionOutput, error)
	DeregisterTaskDefinition(ctx context.Context, params *ecs.DeregisterTaskDefinitionInput, optFns ...func(*ecs.Options)) (*ecs.DeregisterTaskDefinitionOutput, error)
	DeleteTaskDefinitions(ctx context.Context, param *ecs.DeleteTaskDefinitionsInput, optFns ...func(*ecs.Options)) (*ecs.DeleteTaskDefinitionsOutput, error)
	RunTask(ctx context.Context, params *ecs.RunTaskInput, optFns ...func(*ecs.Options)) (*ecs.RunTaskOutput, error)
}

// client is a reusable helper for creating the ECS client using assumed temporary credentials
func (e *ecsRunnerInstance) client(ctx context.Context) (ecsClientSubset, error) {
	runCfg := e.mustRunnerConfiguration()
	exchangedCredentials, err := e.TemporaryAuthProvider.ExchangeAwsCredentials(ctx, e.Runner.OrgId, e.Runner.Id, runCfg.Job.Region, runCfg.Auth)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(opt.OfNonZero(exchangedCredentials.Region).Or(runCfg.Job.Region)),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			exchangedCredentials.AccessKeyID,
			exchangedCredentials.SecretAccessKey,
			exchangedCredentials.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if e.overrideClient != nil {
		return e.overrideClient, nil
	}

	return ecs.NewFromConfig(cfg), nil
}

func (e *ecsRunnerInstance) taskFamily() string {
	return fmt.Sprintf("platform_orchestrator_env_%s_%s", e.Deployment.OrgId, e.Deployment.DeploymentEnvUuid.String())
}

func (e *ecsRunnerInstance) Start(ctx context.Context) error {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	logger := hlogger.TraceScopedLoggerFromCtx(zap.L(), ctx).WithLazy(ids.AsLogField())

	ecsClient, err := e.client(ctx)
	if err != nil {
		return err
	}

	runCfg := e.mustRunnerConfiguration()

	if err := e.deregisterActiveTaskDefinitions(ctx, ecsClient, logger); err != nil {
		return errors.Wrap(err, "failed to deregister active task definitions before starting")
	}
	if err := e.deleteInActiveTaskDefinitions(ctx, ecsClient, logger); err != nil {
		return errors.Wrap(err, "failed to delete in-active task definitions before starting")
	}

	runnerImage := e.getRunnerImage(runCfg)

	td, err := ecsClient.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family:                  ref.Ref(e.taskFamily()),
		ExecutionRoleArn:        ref.Ref(runCfg.Job.ExecutionRoleArn),
		TaskRoleArn:             runCfg.Job.TaskRoleArn,
		Cpu:                     ref.Ref(ecsDefaultCpuUnits),
		Memory:                  ref.Ref(ecsDefaultMemoryMegabytes),
		NetworkMode:             types.NetworkModeAwsvpc,
		RequiresCompatibilities: []types.Compatibility{types.CompatibilityFargate},
		ContainerDefinitions: []types.ContainerDefinition{{
			Name:    ref.Ref(runnercommon.RunnerImageMainContainer),
			Image:   ref.Ref(runnerImage),
			Command: []string{runnercommon.RunnerImageSubCommand},
			Environment: slices.Collect(func(yield func(kv types.KeyValuePair) bool) {
				for k, v := range runnercommon.GenerateEnvVarsForRun(e.MetadataOutputKey, e.Deployment, e.NATSConfiguration) {
					yield(types.KeyValuePair{Name: ref.Ref(k), Value: ref.Ref(v)})
				}
				for _, k := range slices.Sorted(maps.Keys(runCfg.Job.Environment)) {
					yield(types.KeyValuePair{Name: ref.Ref(k), Value: ref.Ref(runCfg.Job.Environment[k])})
				}
			}),
			Secrets: slices.Collect(func(yield func(secret types.Secret) bool) {
				for _, k := range slices.Sorted(maps.Keys(runCfg.Job.Secrets)) {
					yield(types.Secret{
						Name:      ref.Ref(k),
						ValueFrom: ref.Ref(runCfg.Job.Secrets[k]),
					})
				}
			}),
		}},
		Tags: []types.Tag{
			{Key: ref.Ref(ecsManagedByTag), Value: ref.Ref(ecsManagedByTagValue)},
			{Key: ref.Ref(ecsOrgIdTag), Value: ref.Ref(e.Deployment.OrgId)},
			{Key: ref.Ref(ecsProjectIdTag), Value: ref.Ref(e.Deployment.ProjectId)},
			{Key: ref.Ref(ecsEnvIdTag), Value: ref.Ref(e.Deployment.EnvId)},
			{Key: ref.Ref(ecsRunnerIdTag), Value: ref.Ref(e.Runner.Id)},
		},
	})
	if err != nil {
		if ade, ok := aws.IsAccessDeniedException(err); ok {
			return usererrors.NewUserError("assumed role is missing permissions for registering ECS task definitions: " + ade.Err.Error())
		}
		return errors.Wrap(err, "failed to register ECS task definition")
	}
	logger.Info("registered ecs task definition", zap.String("task_definition_arn", *td.TaskDefinition.TaskDefinitionArn))

	if rt, err := ecsClient.RunTask(ctx, &ecs.RunTaskInput{
		ClientToken:    ref.Ref(e.Deployment.Id.String()),
		TaskDefinition: td.TaskDefinition.TaskDefinitionArn,
		Cluster:        ref.Ref(runCfg.Job.Cluster),
		Tags: []types.Tag{
			{Key: ref.Ref(ecsManagedByTag), Value: ref.Ref(ecsManagedByTagValue)},
			{Key: ref.Ref(ecsOrgIdTag), Value: ref.Ref(e.Deployment.OrgId)},
			{Key: ref.Ref(ecsProjectIdTag), Value: ref.Ref(e.Deployment.ProjectId)},
			{Key: ref.Ref(ecsEnvIdTag), Value: ref.Ref(e.Deployment.EnvId)},
			{Key: ref.Ref(ecsRunnerIdTag), Value: ref.Ref(e.Runner.Id)},
			{Key: ref.Ref(ecsDeploymentIdTag), Value: ref.Ref(e.Deployment.Id.String())},
		},
		LaunchType: types.LaunchTypeFargate,
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				AssignPublicIp: ecsAssignPublicIp[runCfg.Job.IsPublicIpEnabled],
				Subnets:        runCfg.Job.Subnets,
				SecurityGroups: runCfg.Job.SecurityGroups,
			},
		},
	}); err != nil {
		if ade, ok := aws.IsAccessDeniedException(err); ok {
			return usererrors.NewUserError("assumed role is missing permissions for running ECS tasks: " + ade.Err.Error())
		} else if oe := new(smithy.OperationError); errors.As(err, &oe) {
			if strings.Contains(oe.Err.Error(), "unable to assume the role") {
				return usererrors.NewUserError("assumed role is missing permissions to pass the execution role to ECS: " + oe.Err.Error())
			}
		}
		return errors.Wrap(err, "failed to run ECS task")
	} else {
		logger.Info("launched ecs task", zap.String("region", runCfg.Job.Region), zap.String("task_arn", *rt.Tasks[0].TaskArn))
	}

	return nil
}

func findTaskWithTag(ctx context.Context, logger *zap.Logger, client ecsClientSubset, cluster, family string, desiredStatus types.DesiredStatus, filterTagKey, filterTagValue string) (*types.Task, error) {
	var nextToken *string
	var candidates int
	for {
		tasks, err := client.ListTasks(ctx, &ecs.ListTasksInput{
			Cluster:       ref.Ref(cluster),
			Family:        ref.Ref(family),
			DesiredStatus: desiredStatus,
			NextToken:     nextToken,
		})
		if err != nil {
			if ade, ok := aws.IsAccessDeniedException(err); ok {
				return nil, usererrors.NewUserError("assumed role is missing permissions for listing ECS tasks: " + ade.Err.Error())
			}
			return nil, errors.Wrap(err, "failed to list ecs tasks")
		}
		nextToken = tasks.NextToken
		candidates += len(tasks.TaskArns)
		if len(tasks.TaskArns) > 0 {
			details, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
				Cluster: ref.Ref(cluster),
				Tasks:   tasks.TaskArns,
				Include: []types.TaskField{types.TaskFieldTags},
			})
			if err != nil {
				if ade, ok := aws.IsAccessDeniedException(err); ok {
					return nil, usererrors.NewUserError("assumed role is missing permissions for describing ECS tasks: " + ade.Err.Error())
				}
				return nil, errors.Wrap(err, "failed to describe ecs tasks")
			}
			ti := slices.IndexFunc(details.Tasks, func(task types.Task) bool {
				return slices.ContainsFunc(task.Tags, func(tag types.Tag) bool {
					return ref.DerefOr(tag.Key, "") == filterTagKey && ref.DerefOr(tag.Value, "") == filterTagValue
				})
			})
			if ti >= 0 {
				logger.Info("found task with desired tag", zap.String("task_arn", *details.Tasks[ti].TaskArn), zap.String("desired_status", string(desiredStatus)), zap.String("filter_tag", filterTagKey), zap.String("filter_value", filterTagValue))
				return &details.Tasks[ti], nil
			}
		}

		if nextToken == nil {
			break
		}
	}
	logger.Info("found no tasks with desired tag", zap.Int("candidates", candidates), zap.String("desired_status", string(desiredStatus)), zap.String("filter_tag", filterTagKey), zap.String("filter_value", filterTagValue))
	return nil, nil
}

func (e *ecsRunnerInstance) IsRunning(ctx context.Context) (bool, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	logger := hlogger.TraceScopedLoggerFromCtx(zap.L(), ctx).WithLazy(ids.AsLogField())

	ecsClient, err := e.client(ctx)
	if err != nil {
		return false, err
	}

	runCfg := e.mustRunnerConfiguration()
	task, err := findTaskWithTag(ctx, logger, ecsClient, runCfg.Job.Cluster, e.taskFamily(), types.DesiredStatusRunning, ecsDeploymentIdTag, e.Deployment.Id.String())
	if err != nil {
		return false, err
	} else if task == nil {
		return false, nil
	}
	_, isRunning := ecsTaskRunningStatus[ref.DerefOr(task.LastStatus, "")]
	logger.Info("checking run status of ecs task", zap.String("task_arn", *task.TaskArn), zap.String("last_status", *task.LastStatus), zap.String("desired_status", *task.DesiredStatus))
	return isRunning, nil
}

func (e *ecsRunnerInstance) CheckStatus(ctx context.Context) (*RunnerStatus, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	logger := hlogger.TraceScopedLoggerFromCtx(zap.L(), ctx).WithLazy(ids.AsLogField())

	ecsClient, err := e.client(ctx)
	if err != nil {
		return nil, err
	}

	runCfg := e.mustRunnerConfiguration()

	// Search for a running task first.
	task, err := findTaskWithTag(ctx, logger, ecsClient, runCfg.Job.Cluster, e.taskFamily(), types.DesiredStatusRunning, ecsDeploymentIdTag, e.Deployment.Id.String())
	if err != nil {
		return nil, err
	}
	// If no running task is found, search for a stopped one.
	if task == nil {
		task, err = findTaskWithTag(ctx, logger, ecsClient, runCfg.Job.Cluster, e.taskFamily(), types.DesiredStatusStopped, ecsDeploymentIdTag, e.Deployment.Id.String())
		if err != nil {
			return nil, err
		}
	}

	if task == nil {
		return &RunnerStatus{IsCompleted: true, IsStuck: false, Message: "task not found"}, nil
	}
	lastStatus := ref.DerefOr(task.LastStatus, "")
	logger.Info("checking status of ecs task", zap.String("task_arn", *task.TaskArn), zap.String("last_status", lastStatus), zap.String("desired_status", *task.DesiredStatus))

	// If the task is running, then we're not completed and not stuck.
	if task.StartedAt != nil && task.StoppingAt == nil && task.StoppedAt == nil {
		return &RunnerStatus{IsCompleted: false, IsStuck: false}, nil
	}

	// Then we're stuck if we haven't started yet and the timeout has been reached.
	stuckStarting := task.StartedAt == nil && time.Since(ref.DerefOr(task.CreatedAt, time.Time{})) > ecsSchedulingTimeout
	// Or we've taken too long to stop
	stuckStopping := task.StoppedAt == nil && task.StoppingAt != nil && time.Since(ref.DerefOr(task.StoppingAt, time.Time{})) > ecsSchedulingTimeout
	if stuckStarting || stuckStopping {
		return &RunnerStatus{IsCompleted: false, IsStuck: true, Message: fmt.Sprintf(
			"ECS runner task is stuck in status %s (see https://%s.console.aws.amazon.com/ecs/v2/clusters/%s/tasks/%s for details)",
			lastStatus, runCfg.Job.Region, runCfg.Job.Cluster, (*task.TaskArn)[strings.LastIndex(*task.TaskArn, "/")+1:],
		)}, nil
	}

	if task.StoppedAt != nil {
		if task.StopCode != types.TaskStopCodeEssentialContainerExited {
			return &RunnerStatus{IsCompleted: false, IsStuck: true, Message: fmt.Sprintf(
				"ECS runner task stopped unexpectedly (see https://%s.console.aws.amazon.com/ecs/v2/clusters/%s/tasks/%s for details)",
				runCfg.Job.Region, runCfg.Job.Cluster, (*task.TaskArn)[strings.LastIndex(*task.TaskArn, "/")+1:],
			)}, nil
		}
		return &RunnerStatus{IsCompleted: true, IsStuck: false}, nil
	}
	return &RunnerStatus{IsCompleted: false, IsStuck: false}, nil
}

func (e *ecsRunnerInstance) deregisterActiveTaskDefinitions(ctx context.Context, client ecsClientSubset, logger *zap.Logger) error {
	// NOTE: this only returns 1 page (100) items, and we don't follow pagination here. This is intentionally to bound the complexity of the method
	// and also because we only create 1 at a time and always run this cleanup first, we'll never have more than 100 active task definitions.
	listRes, err := client.ListTaskDefinitions(ctx, &ecs.ListTaskDefinitionsInput{
		FamilyPrefix: ref.Ref(e.taskFamily()),
		Status:       types.TaskDefinitionStatusActive,
	})
	if err != nil {
		if ade, ok := aws.IsAccessDeniedException(err); ok {
			return usererrors.NewUserError("assumed role is missing permissions for listing ECS task definitions: " + ade.Err.Error())
		}
		return errors.Wrap(err, "failed to list task definitions")
	}
	logger.Info("listed active task definitions", zap.Int("count", len(listRes.TaskDefinitionArns)), zap.Bool("more", listRes.NextToken != nil))
	if len(listRes.TaskDefinitionArns) > ecsDeleteStaleTaskDefinitionLimit {
		listRes.TaskDefinitionArns = listRes.TaskDefinitionArns[:ecsDeleteStaleTaskDefinitionLimit]
	}
	for _, td := range listRes.TaskDefinitionArns {
		deregisterRes, err := client.DeregisterTaskDefinition(ctx, &ecs.DeregisterTaskDefinitionInput{
			TaskDefinition: ref.Ref(td),
		})
		if err != nil {
			if ade, ok := aws.IsAccessDeniedException(err); ok {
				return usererrors.NewUserError("assumed role is missing permissions for deregistering ECS task definitions: " + ade.Err.Error())
			}
			return errors.Wrapf(err, "failed to deregister task definition '%s'", td)
		}
		logger.Info("deregistered ecs task definition", zap.String("task_definition_arn", ref.DerefOr(ref.DerefOr(deregisterRes.TaskDefinition, types.TaskDefinition{}).TaskDefinitionArn, "")))
	}
	return nil
}

func (e *ecsRunnerInstance) deleteInActiveTaskDefinitions(ctx context.Context, client ecsClientSubset, logger *zap.Logger) error {
	// NOTE: this only returns 1 page (100) items, and we can only delete 10 at a time, so we don't follow pagination here. This is intentionally to bound the complexity of the method
	// and also because we only create 1 at a time and always run this cleanup first, we should always converge on only 1 active task definition.
	listRes, err := client.ListTaskDefinitions(ctx, &ecs.ListTaskDefinitionsInput{
		FamilyPrefix: ref.Ref(e.taskFamily()),
		Status:       types.TaskDefinitionStatusInactive,
	})
	if err != nil {
		if ade, ok := aws.IsAccessDeniedException(err); ok {
			return usererrors.NewUserError("assumed role is missing permissions for listing ECS task definitions: " + ade.Err.Error())
		}
		return errors.Wrap(err, "failed to list task definitions")
	}
	logger.Info("listed inactive task definitions", zap.Int("count", len(listRes.TaskDefinitionArns)), zap.Bool("more", listRes.NextToken != nil))
	if len(listRes.TaskDefinitionArns) == 0 {
		return nil
	}
	if len(listRes.TaskDefinitionArns) > ecsDeleteStaleTaskDefinitionLimit {
		listRes.TaskDefinitionArns = listRes.TaskDefinitionArns[:ecsDeleteStaleTaskDefinitionLimit]
	}
	deleteRes, err := client.DeleteTaskDefinitions(ctx, &ecs.DeleteTaskDefinitionsInput{
		TaskDefinitions: listRes.TaskDefinitionArns,
	})
	if err != nil {
		if ade, ok := aws.IsAccessDeniedException(err); ok {
			return usererrors.NewUserError("assumed role is missing permissions for deleting ECS task definitions: " + ade.Err.Error())
		}
		return errors.Wrap(err, "failed to delete task definitions")
	}
	logger.Info("deleted task definitions", zap.Int("count", len(deleteRes.TaskDefinitions)))
	return nil
}

var _ RunnerInterface = (*ecsRunnerInstance)(nil)

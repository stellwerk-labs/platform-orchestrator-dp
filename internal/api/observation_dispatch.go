package api

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const nativeObservationStrategy = "observation"

// DispatchObservation sends only the dedicated read-only observer, never a customer recipe.
func (p *PipelineDispatcher) DispatchObservation(ctx context.Context, dep *model.DeploymentSummary) error {
	if dep.NativeTarget != nil {
		return p.dispatchNativeObservation(ctx, dep)
	}
	if dep.Mode != model.DeploymentModeObservation {
		return errors.New("native observer requires observation mode")
	}
	if p.Server.RunnerRollouts == nil || dep.OrgId != p.Server.RunnerRollouts.OrgID || dep.RunnerId != p.Server.RunnerRollouts.RunnerID {
		return errors.New("native observation site is not configured")
	}
	if err := p.reserveCapacity(ctx, dep); err != nil {
		return err
	}
	command, err := p.frozenObservationCommand(ctx, dep)
	if err != nil {
		return err
	}
	var envelope hmessaging.CommandEnvelope
	if err = json.Unmarshal(command, &envelope); err != nil {
		return err
	}
	subject, err := hmessaging.RunnerCommandSubject(dep.OrgId, dep.RunnerId)
	if err != nil {
		return err
	}
	return p.Publisher.Publish(ctx, hmessaging.Message{ID: envelope.CommandID, Subject: subject, Data: command, CreatedAt: envelope.CreatedAt, ExpiresAt: envelope.ExpiresAt})
}

func (p *PipelineDispatcher) dispatchNativeObservation(ctx context.Context, dep *model.DeploymentSummary) error {
	if dep.Mode != model.DeploymentModeObservation || dep.HasPipeline || !validNativeDispatchTarget(dep) {
		return model.NewErrConflict("Native observation target is invalid")
	}
	command, err := p.frozenNativeObservationCommand(ctx, dep)
	if err != nil {
		return err
	}
	var envelope hmessaging.CommandEnvelope
	if err = json.Unmarshal(command, &envelope); err != nil {
		return err
	}
	subject, err := hmessaging.RunnerCommandSubject(dep.OrgId, envelope.RunnerID)
	if err != nil {
		return err
	}
	id, err := hmessaging.NativeMessageID(dep.OrgId, envelope.RunnerID, "commands", envelope.Type, envelope.CommandID)
	if err != nil {
		return err
	}
	return p.Publisher.Publish(ctx, hmessaging.Message{ID: id, Subject: subject, Data: command, CreatedAt: envelope.CreatedAt, ExpiresAt: envelope.ExpiresAt})
}

func (p *PipelineDispatcher) frozenNativeObservationCommand(ctx context.Context, dep *model.DeploymentSummary) ([]byte, error) {
	tx, err := p.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = lockPipelineDispatchDeployment(ctx, tx, dep); err != nil {
		return nil, err
	}
	var command []byte
	var createdAt time.Time
	if err = tx.QueryRowContext(ctx, `SELECT b.observation_command,d.created_at FROM deployment_dispatch_budgets b JOIN deployments d ON d.id=b.deployment_id WHERE b.deployment_id=$1 FOR UPDATE OF b`, dep.Id).Scan(&command, &createdAt); err != nil {
		return nil, err
	}
	// Admission already owns capacity. Reacquiring it here would invert the
	// Runner advisory-lock and Deployment lock order and consult mutable policy.
	if err = verifyPipelineDispatchOwnership(ctx, tx, dep, len(command) != 0); err != nil {
		return nil, err
	}
	if len(command) != 0 {
		if err = validateFrozenObservationCommand(command, dep); err != nil {
			return nil, err
		}
		return command, nil
	}
	if !time.Now().Before(createdAt.Add(model.RunnerExecutionBudget)) {
		return nil, model.NewErrConflict("observation preparation expired before Runner dispatch")
	}
	definition := model.PipelineDefinition{Strategy: nativeObservationStrategy, Steps: []model.PipelineStep{}, RequiredCapabilities: []string{"environment-observation/v1"}}
	command, err = p.buildCommand(ctx, tx, dep, definition, string(dep.NativeTarget.StateBackendType))
	if err != nil {
		return nil, err
	}
	if err = validateFrozenObservationCommand(command, dep); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET observation_command=$2,first_attempt_at=now(),deadline=now()+interval '24 hours' WHERE deployment_id=$1`, dep.Id, command); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return command, nil
}

func validateFrozenObservationCommand(raw []byte, dep *model.DeploymentSummary) error {
	payload, err := model.FrozenPipelinePayload(raw, dep)
	if err != nil {
		return err
	}
	var command pipelineCommand
	if json.Unmarshal(payload, &command) != nil || command.Strategy != nativeObservationStrategy ||
		!slices.Contains(command.RequiredCapabilities, "environment-observation/v1") ||
		command.Recipe != nil || command.Cost != nil || command.PlanBase != nil || command.SourcePlan != nil ||
		len(command.Steps) != 0 || command.Approval != (model.PipelineApproval{}) ||
		command.Request.MetadataKey != "" || command.Request.EncryptedOutputsRecipient != "" {
		return model.NewErrConflict("frozen observation command authority unavailable")
	}
	return nil
}

func (p *PipelineDispatcher) frozenObservationCommand(ctx context.Context, dep *model.DeploymentSummary) ([]byte, error) {
	tx, err := p.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var existing []byte
	if err = tx.QueryRowContext(ctx, `SELECT observation_command FROM deployment_dispatch_budgets WHERE deployment_id=$1 FOR UPDATE`, dep.Id).Scan(&existing); err != nil {
		return nil, err
	}
	if len(existing) > 0 {
		return existing, nil
	}
	backend, err := p.observationBackend(ctx, dep)
	if err != nil {
		return nil, err
	}
	definition := model.PipelineDefinition{Strategy: nativeObservationStrategy, Steps: []model.PipelineStep{}, RequiredCapabilities: []string{"environment-observation/v1"}}
	command, err := p.buildCommand(ctx, tx, dep, definition, backend)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET observation_command=$2,first_attempt_at=COALESCE(first_attempt_at,now()),deadline=COALESCE(deadline,now()+interval '24 hours') WHERE deployment_id=$1`, dep.Id, command); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return command, nil
}

func (p *PipelineDispatcher) observationBackend(ctx context.Context, dep *model.DeploymentSummary) (string, error) {
	runner, err := p.Server.ControlPlaneClient.GetRunnerWithResponse(ctx, dep.OrgId, dep.RunnerId)
	if err != nil || runner.JSON200 == nil {
		return "", errors.New("observation Runner registration unavailable")
	}
	if err := validateNativeJobConfiguration(runner.JSON200.RunnerConfiguration); err != nil {
		return "", err
	}
	_, supported, err := model.RunnerAdmissionConfiguration(runner.JSON200.RunnerConfiguration)
	if err != nil {
		return "", err
	}
	if !supported {
		return "", model.NewErrConflict("Runner does not support dedicated read-only observation")
	}
	backend, err := runner.JSON200.StateStorageConfiguration.Discriminator()
	if err != nil {
		return "", err
	}
	return backend, nil
}

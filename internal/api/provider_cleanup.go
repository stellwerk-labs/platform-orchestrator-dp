package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const maxProviderHistoryAttempts = 32

// A failed apply can leave resources from any attempt since the last successful
// state change. The latest failed graph alone need not describe them all.
// The boolean proves a successful state boundary, required for deletion but
// deliberately not required to compile cleanup from a first partial attempt.
func providerStateHistory(ctx context.Context, db model.Databaser, tx model.Tx, head *model.DeploymentSummary, initial graphs.ProviderSnapshot) ([]graphs.ProviderSnapshot, bool, error) {
	if head == nil || model.IsPlanDeployment(head.Mode) {
		return nil, false, nil
	}
	if head.Mode == model.DeploymentModeDestroy && head.Status == model.DeploymentStatusSucceeded {
		return nil, true, nil
	}
	initial.RunnerID = head.RunnerId
	initial.MayBePartial = head.Status != model.DeploymentStatusSucceeded
	history := []graphs.ProviderSnapshot{initial}
	if !initial.MayBePartial {
		return history, true, nil
	}
	prior, more, err := db.ListDeployments(ctx, tx, head.OrgId, "", maxProviderHistoryAttempts+1, model.ListDeploymentsParams{
		ProjectId: opt.Of(head.ProjectId), EnvId: opt.Of(head.EnvId),
		ByMode: []model.DeploymentMode{model.DeploymentModeDeploy, model.DeploymentModeRollback, model.DeploymentModeDestroy},
	})
	if err != nil {
		return nil, false, err
	}
	if len(prior) == 0 || prior[0].Id != head.Id {
		return nil, false, model.NewErrConflict("cannot verify the order of state deployments; retry against the current Environment")
	}
	newer := head.CreatedAt
	for _, attempt := range prior[1:] {
		if !attempt.CreatedAt.Before(newer) {
			return nil, false, model.NewErrConflict("cannot verify the order of state deployments")
		}
		newer = attempt.CreatedAt
		if attempt.DeploymentEnvUuid != head.DeploymentEnvUuid {
			return nil, false, model.NewErrConflict("cannot determine provider cleanup context across different Environment identities")
		}
		if attempt.Status == model.DeploymentStatusSucceeded && attempt.Mode == model.DeploymentModeDestroy {
			return history, true, nil
		}
		_, _, tofu, graph, err := db.GetDeployment(ctx, tx, head.OrgId, attempt.Id, model.GetModeDefault)
		if err != nil {
			return nil, false, err
		}
		history = append(history, graphs.ProviderSnapshot{Graph: graph, Tofu: tofu, RunnerID: attempt.RunnerId, MayBePartial: attempt.Status != model.DeploymentStatusSucceeded})
		if attempt.Status == model.DeploymentStatusSucceeded {
			return history, true, nil
		}
	}
	if more != "" {
		return nil, false, model.NewErrConflict("too many unresolved deployment attempts to determine provider cleanup context; reconcile the Environment state before retrying")
	}
	return history, false, nil
}

func currentProviderState(ctx context.Context, db model.Databaser, tx model.Tx, org, project, environment string, environmentUUID uuid.UUID) (*model.DeploymentSummary, graphs.ProviderSnapshot, error) {
	head, _, tofu, graph, err := db.GetLastDeployment(ctx, tx, org, project, environment, model.GetLastDeploymentParams{StateChangeOnly: true})
	if _, absent := model.IsErrNotFound(err); absent {
		return nil, graphs.ProviderSnapshot{}, nil
	}
	if err != nil {
		return nil, graphs.ProviderSnapshot{}, err
	}
	if head.DeploymentEnvUuid != environmentUUID {
		return nil, graphs.ProviderSnapshot{}, model.NewErrConflict("provider cleanup state belongs to a different Environment")
	}
	return head, graphs.ProviderSnapshot{Graph: graph, Tofu: tofu}, nil
}

func loadCleanupProviders(ctx context.Context, client cp.ClientWithResponsesInterface, org, project, environment string, ids []string, editors []cp.RequestEditorFn) ([]cp.ModuleProvider, error) {
	if client == nil {
		return nil, fmt.Errorf("the authenticated material catalogue is unavailable")
	}
	response, err := client.GenerateInternalExecutionCatalogueWithResponse(ctx, org, project, environment, nil, cp.InternalExecutionCatalogueGenerateBody{
		AreRulesIgnored: true, MaterialOnly: true, PinnedProviders: ids,
	}, editors...)
	if err != nil || response == nil || response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		return nil, fmt.Errorf("the historical provider bindings are unavailable from the material catalogue")
	}
	return response.JSON200.Providers, nil
}

package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"maps"
	"slices"

	"github.com/google/uuid"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const providerUsageEnvironmentLimit = 10000
const providerUsagePageSize = 100

// InternalCheckProviderUsage accepts only Core's internal system identity.
// A partial or unprotected scan cannot authorize GC.
func (s *Server) InternalCheckProviderUsage(ctx context.Context, request InternalCheckProviderUsageRequestObject) (InternalCheckProviderUsageResponseObject, error) {
	actor, authErr := GetAuthenticatedUserIdOr401(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if actor != userid.InternalSystemUuid {
		return nil, echo.NewHTTPError(http.StatusForbidden)
	}

	if s.DeploymentAdmissions == nil || s.DeploymentAdmissions.Core == nil || s.DeploymentAdmissions.BearerToken == "" {
		return InternalCheckProviderUsage503Response{}, nil
	}
	report, err := scanProviderUsage(ctx, s.Database, request.OrgId, request.Body.RequiredEnvironmentUuids)
	if err != nil {
		// Historical configuration can contain credentials. Return no parser or
		// database details in this evidence endpoint.
		return InternalCheckProviderUsage503Response{}, nil
	}
	return InternalCheckProviderUsage200JSONResponse(report), nil
}

func scanProviderUsage(ctx context.Context, db model.Databaser, org string, expected []uuid.UUID) (InternalProviderUsage, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return InternalProviderUsage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	providers := make(map[string]bool)
	report := InternalProviderUsage{ProtocolVersion: 1, OrgId: org, Complete: true, EnvironmentUuids: []uuid.UUID{}}
	if err := model.CheckProviderStateCoverage(ctx, tx, org); err != nil {
		return InternalProviderUsage{}, err
	}
	page := ""
	for {
		heads, next, err := db.ListLastDeployments(ctx, tx, org, page, providerUsagePageSize, model.ListLastDeploymentsParams{StateChangeOnly: true})
		if err != nil {
			return InternalProviderUsage{}, err
		}
		for _, head := range heads {
			report.EnvironmentsScanned++
			report.EnvironmentUuids = append(report.EnvironmentUuids, head.DeploymentEnvUuid)
			if report.EnvironmentsScanned > providerUsageEnvironmentLimit {
				return InternalProviderUsage{}, fmt.Errorf("provider usage scan limit exceeded")
			}
			if head.Status == model.DeploymentStatusExecuting || head.EnvironmentFence.IsSet() {
				report.Busy = true
			}
			_, _, tofu, graph, err := db.GetDeployment(ctx, tx, org, head.Id, model.GetModeDefault)
			if err != nil {
				return InternalProviderUsage{}, err
			}
			history, baselineKnown, err := providerStateHistory(ctx, db, tx, &head, graphs.ProviderSnapshot{Graph: graph, Tofu: tofu})
			if err != nil {
				return InternalProviderUsage{}, err
			}
			if !baselineKnown && slices.Contains(expected, head.DeploymentEnvUuid) {
				return InternalProviderUsage{}, fmt.Errorf("required Environment has no successful state boundary")
			}
			ids, err := graphs.StateProviderIDs(history)
			if err != nil {
				return InternalProviderUsage{}, err
			}
			for _, id := range ids {
				providers[id] = true
			}
		}
		if next == "" {
			break
		}
		if next == page {
			return InternalProviderUsage{}, fmt.Errorf("provider usage pagination did not advance")
		}
		page = next
	}
	if err := scanEmptyEffectiveState(ctx, tx, org, &report); err != nil {
		return InternalProviderUsage{}, err
	}
	for _, environment := range expected {
		if !slices.Contains(report.EnvironmentUuids, environment) {
			return InternalProviderUsage{}, fmt.Errorf("required Environment state is unavailable")
		}
	}
	report.Providers = append([]string{}, slices.Sorted(maps.Keys(providers))...)
	return report, nil
}

// A proven pre-effect failure can be the only Deployment of an Environment.
// Count that explicit empty state as coverage, rather than inventing a head or
// interpreting an absent row as evidence that provider material is unused.
func scanEmptyEffectiveState(ctx context.Context, tx model.Tx, org string, report *InternalProviderUsage) error {
	rows, err := tx.QueryContext(ctx, `SELECT e.id,
 EXISTS(SELECT 1 FROM deployments d WHERE d.de_id=e.id AND d.status='executing')
 OR EXISTS(SELECT 1 FROM runner_job_reservations r JOIN deployments d ON d.id=r.deployment_id WHERE d.de_id=e.id AND r.released_at IS NULL)
 FROM deployment_environments e JOIN deployment_effective_state effective ON effective.environment_id=e.id
 WHERE e.org_id=$1 AND effective.deployment_id IS NULL ORDER BY e.id LIMIT $2`, org, providerUsageEnvironmentLimit+1)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var environment uuid.UUID
		var busy bool
		if err := rows.Scan(&environment, &busy); err != nil {
			return err
		}
		report.EnvironmentsScanned++
		if report.EnvironmentsScanned > providerUsageEnvironmentLimit {
			return fmt.Errorf("provider usage scan limit exceeded")
		}
		report.EnvironmentUuids = append(report.EnvironmentUuids, environment)
		report.Busy = report.Busy || busy
	}
	return rows.Err()
}

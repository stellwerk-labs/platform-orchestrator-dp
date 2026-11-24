package api

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

func (s *Server) pipelineSummaries(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*DeploymentPipelineSummary, error) {
	result := make(map[uuid.UUID]*DeploymentPipelineSummary)
	if len(ids) == 0 {
		return result, nil
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = id.String()
	}
	rows, err := tx.QueryContext(ctx, `SELECT deployment_id,jsonb_build_object(
'id',definition->>'id','version',definition->'version','name',definition->>'name',
'strategy',definition->>'strategy','phase',progress->>'phase',
'infrastructure_applied',COALESCE(progress->'infrastructure_applied','false'::jsonb))
FROM deployment_pipeline_runs WHERE deployment_id=ANY($1::uuid[])`, pq.Array(values))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id uuid.UUID
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		var summary DeploymentPipelineSummary
		if err = json.Unmarshal(raw, &summary); err != nil {
			return nil, err
		}
		result[id] = &summary
	}
	return result, rows.Err()
}

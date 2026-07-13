package model

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

type ResourceNode struct {
	Hash              string
	DeploymentEnvUuid uuid.UUID
	ResourceType      string
	ResourceClass     string
	ResourceId        string

	LastDeploymentId            uuid.UUID
	LastModuleDefinitionId      string
	LastModuleDefinitionVersion string

	Edges map[string]string

	Metadata map[string]interface{}
}

type UpdateResourceNodeParams struct {
	Hash     string
	Metadata map[string]interface{}
}

func (d *databaser) GetActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid uuid.UUID) ([]ResourceNode, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)
	tx := d.txOrDb(optionalTx)
	if rows, err := tx.QueryContext(
		ctx,
		`SELECT 
    -- all the columns
    n.hash, n.resource_type, n.resource_class, n.resource_id, n.last_deployment_id, n.last_module_definition_id, n.last_module_definition_version, 
    -- and all the edges using COALESCE over the LEFT JOIN + GROUP BY. the filter ensures we don't have a null in the edge list when no edges exist
    COALESCE(json_object_agg(e.edge_alias, e.target_hash) FILTER (WHERE e.edge_alias IS NOT NULL), '{}'::json) AS edges, n.metadata 
-- we join on all edges that come from the same node and only include those that are up to date with the deployment id on the node itself, so we're never showing edges between new and old nodes.
FROM resource_nodes n LEFT JOIN resource_nodes_depends_on e ON e.subject_hash = n.hash AND e.last_deployment_id = n.last_deployment_id
-- finally only include the nodes that are in the target environment, and our group by to support the edge coalesce comes last.
WHERE n.env_uuid = $1 GROUP BY n.hash`,
		deploymentEnvUuid,
	); err != nil {
		return nil, errors.Wrap(err, "failed to query resource nodes")
	} else {
		defer func() {
			if err := rows.Close(); err != nil {
				logger.Error("failed to close row set", zap.Error(err))
			}
		}()

		out := make([]ResourceNode, 0)
		for rows.Next() {
			next := ResourceNode{
				DeploymentEnvUuid: deploymentEnvUuid,
			}
			if err := rows.Scan(&next.Hash, &next.ResourceType, &next.ResourceClass, &next.ResourceId, &next.LastDeploymentId, &next.LastModuleDefinitionId, &next.LastModuleDefinitionVersion, asJson(&next.Edges), asJson(&next.Metadata)); err != nil {
				return nil, errors.Wrap(err, "failed to scan row")
			}
			out = append(out, next)
		}
		if err := rows.Err(); err != nil {
			return nil, errors.Wrap(err, "failed to iterate rows")
		}
		return out, nil
	}
}

func (d *databaser) DiscardOldActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid uuid.UUID, deploymentId uuid.UUID) error {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)
	tx := d.txOrDb(optionalTx)
	if rs, err := tx.ExecContext(
		ctx,
		`DELETE FROM resource_nodes WHERE env_uuid = $1 AND last_deployment_id != $2`,
		deploymentEnvUuid, deploymentId,
	); err != nil {
		return errors.Wrap(err, "failed to delete old resource nodes")
	} else if rc, err := rs.RowsAffected(); err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	} else {
		logger.Info("removed old resource node edges", zap.Int64("rows_affected", rc))
	}
	return nil
}

func (d *databaser) InitActiveResourcesFromGraph(ctx context.Context, tx Tx, deploymentEnvUuid, deploymentId uuid.UUID, graph *platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]) error {
	if tx == nil {
		return errors.New("transaction is required")
	}
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	nodeHashes := make(map[platform_orchestrator_graph.ResourceCoordinate]string, len(graph.Nodes))
	for c := range graph.Nodes {
		nodeHashes[c] = util.GenerateNodeHash(deploymentEnvUuid, c.Type, c.Class, c.Id)
	}

	hashes := make([]string, 0, len(graph.Nodes))
	resourceTypes := make([]string, 0, len(graph.Nodes))
	resourceClasses := make([]string, 0, len(graph.Nodes))
	resourceIds := make([]string, 0, len(graph.Nodes))
	moduleDefs := make([]string, 0, len(graph.Nodes))
	moduleVersions := make([]string, 0, len(graph.Nodes))

	subjectHashes := make([]string, 0, len(graph.Edges))
	targetHashes := make([]string, 0, len(graph.Edges))
	edgeAliases := make([]string, 0, len(graph.Edges))

	for c, node := range graph.Nodes {
		if node.ModuleConfiguration == nil {
			// TODO: I think this is due to the workload nodes not having a real module definition at the time of writing
			moduleDefs = append(moduleDefs, "")
			moduleVersions = append(moduleVersions, "")
		} else {
			// skip deleted nodes
			if node.ModuleConfiguration.Deleted {
				continue
			}

			moduleDefs = append(moduleDefs, node.ModuleConfiguration.DefinitionId)
			moduleVersions = append(moduleVersions, node.ModuleConfiguration.VersionId)
		}

		hashes = append(hashes, nodeHashes[c])
		resourceTypes = append(resourceTypes, c.Type)
		resourceClasses = append(resourceClasses, c.Class)
		resourceIds = append(resourceIds, c.Id)

		for alias, target := range graph.Edges[c] {
			if th, ok := nodeHashes[target]; ok {
				subjectHashes = append(subjectHashes, nodeHashes[c])
				targetHashes = append(targetHashes, th)
				edgeAliases = append(edgeAliases, alias)
			} else {
				logger.Info("skipping active resource edge because target node doesn't exist", zap.Stringer("source", c), zap.String("alias", alias), zap.Stringer("target", target))
			}
		}
	}

	// Here we insert all the nodes if they don't exist, if they do exist, then we just update their definition and deployment id.
	// Old nodes from previous deployments that were not in this graph, are not updated.
	if len(hashes) > 0 {
		if rs, err := tx.ExecContext(
			ctx,
			`INSERT INTO resource_nodes (hash, env_uuid, last_deployment_id, last_module_definition_id, last_module_definition_version, resource_type, resource_class, resource_id)
-- this is a bulk insert, so we use unnest to pass in the hashes, resource types, etc.
SELECT a, $2, $3, b, c, d, e, f FROM unnest($1::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[]) AS x (a, b, c, d, e, f)
-- if the node already exists, we update the deployment id and module definition
ON CONFLICT (hash) DO UPDATE SET last_deployment_id = EXCLUDED.last_deployment_id, last_module_definition_id = EXCLUDED.last_module_definition_id, last_module_definition_version = EXCLUDED.last_module_definition_version`,
			pq.Array(hashes), deploymentEnvUuid, deploymentId, pq.Array(moduleDefs), pq.Array(moduleVersions), pq.Array(resourceTypes), pq.Array(resourceClasses), pq.Array(resourceIds),
		); err != nil {
			return errors.Wrap(err, "failed to insert resource nodes")
		} else if rc, err := rs.RowsAffected(); err != nil {
			return errors.Wrap(err, "failed to get rows affected")
		} else {
			logger.Info("inserted resource nodes", zap.Int64("rows_affected", rc))
		}

		// Here we insert the new edges from the current deployment if they don't exist or just update the deployment id and alias.
		if rs, err := tx.ExecContext(
			ctx,
			`INSERT INTO resource_nodes_depends_on (subject_hash, target_hash, last_deployment_id, edge_alias)
-- this is a bulk insert, so we use unnest to pass in the hashes, deployment id, etc.
SELECT a, b, $3, c FROM unnest($1::text[], $2::text[], $4::text[]) AS x (a, b, c)
-- if the edge already exists, we update the deployment id and alias
ON CONFLICT (subject_hash, target_hash) DO UPDATE SET last_deployment_id = EXCLUDED.last_deployment_id, edge_alias = EXCLUDED.edge_alias
`,
			pq.Array(subjectHashes), pq.Array(targetHashes), deploymentId, pq.Array(edgeAliases),
		); err != nil {
			return errors.Wrap(err, "failed to insert resource node dependencies")
		} else if rc, err := rs.RowsAffected(); err != nil {
			return errors.Wrap(err, "failed to get rows affected")
		} else {
			logger.Info("inserted resource node edges", zap.Int64("rows_affected", rc))
		}
	}

	// Finally, we want to detach new/current nodes from old nodes that will be deleted, since there is obviously no dependency relationship there.
	// This leaves us with disjoint graphs.
	if rs, err := tx.ExecContext(
		ctx,
		`DELETE FROM resource_nodes_depends_on WHERE (subject_hash, target_hash) IN
-- finally a bulk delete of edges between old and new nodes
(SELECT subject_hash, target_hash FROM resource_nodes_depends_on e INNER JOIN resource_nodes n ON n.env_uuid = $1 AND e.subject_hash = n.hash WHERE n.last_deployment_id = $2 AND e.last_deployment_id != $2)`,
		deploymentEnvUuid, deploymentId,
	); err != nil {
		return errors.Wrap(err, "failed to delete resource node dependencies")
	} else if rc, err := rs.RowsAffected(); err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	} else {
		logger.Info("removed old resource node edges", zap.Int64("rows_affected", rc))
	}

	return nil
}

func (d *databaser) BulkUpdateActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid, deploymentId uuid.UUID, params []UpdateResourceNodeParams) error {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	hashes := make([]string, len(params))
	metadatas := make([]*string, len(params))

	for i, p := range params {
		hashes[i] = p.Hash
		if p.Metadata != nil {
			jsonBytes, err := json.Marshal(p.Metadata)
			if err != nil {
				return errors.Wrap(err, "failed to marshal metadata")
			}
			metadatas[i] = ref.Ref(string(jsonBytes))
		}
	}

	rs, err := d.txOrDb(optionalTx).ExecContext(
		ctx, `
UPDATE resource_nodes rn
SET metadata = COALESCE(bulk.metadata, rn.metadata)
FROM (
    SELECT unnest($1::text[]) AS hash, unnest($2::jsonb[]) AS metadata
) AS bulk
WHERE rn.hash = bulk.hash AND rn.last_deployment_id = $3 AND rn.env_uuid = $4;
`,
		pq.Array(hashes), pq.Array(metadatas), deploymentId, deploymentEnvUuid,
	)
	if err != nil {
		return errors.Wrap(err, "failed to update resource nodes")
	}

	rc, err := rs.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}
	logger.Info("updated resource nodes", zap.Int64("rows_affected", rc))
	return nil
}

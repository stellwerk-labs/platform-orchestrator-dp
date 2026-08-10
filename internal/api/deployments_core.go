package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	cperrors "github.com/stellwerk-labs/platform-orchestrator-cp/shared/errcodes"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

// convertDpManifestToGraphManifest converts the deployment manifest from the API into the graph used by the platformorchestratorgraph
// library. We clone maps where necessary.
func convertDpManifestToGraphManifest(m DeploymentManifest) platformorchestratorgraph.Manifest {
	workloads := make(map[string]platformorchestratorgraph.ManifestWorkload, len(m.Workloads))
	for workloadName, workload := range m.Workloads {
		resources := make(map[string]platformorchestratorgraph.ManifestResource, len(workload.Resources))
		for alias, resource := range workload.Resources {
			resources[alias] = platformorchestratorgraph.ManifestResource{
				Type:   resource.Type,
				Class:  platformorchestratorgraph.OptionalStringOfRef(resource.Class),
				Id:     platformorchestratorgraph.OptionalStringOfRef(resource.Id),
				Params: maps.Clone(resource.Params),
			}
		}
		workloads[workloadName] = platformorchestratorgraph.ManifestWorkload{
			Resources: resources,
			Outputs:   maps.Clone(workload.Outputs),
		}
	}
	var sharedResources map[string]platformorchestratorgraph.ManifestResource
	if len(m.Shared) > 0 {
		sharedResources = make(map[string]platformorchestratorgraph.ManifestResource, len(m.Shared))
		for alias, resource := range m.Shared {
			sharedResources[alias] = platformorchestratorgraph.ManifestResource{
				Type:   resource.Type,
				Class:  platformorchestratorgraph.OptionalStringOfRef(resource.Class),
				Id:     platformorchestratorgraph.OptionalStringOfRef(resource.Id),
				Params: maps.Clone(resource.Params),
			}
		}
	}
	return platformorchestratorgraph.Manifest{
		Workloads:       workloads,
		SharedResources: sharedResources,
	}
}

func CreateDeployment(
	ctx context.Context, logger *zap.Logger, createdBy uuid.UUID,
	orgId, projectId, envId string, env platformorchestratorcp.Environment, mode model.DeploymentMode, rollbackTo opt.Opt[uuid.UUID], manifest DeploymentManifest,
	encryptedOutputsRecipient, encryptedLogsRecipient, idempotencyKey *string, dryRun bool, runnerLogLevel *DeploymentCreateBodyRunnerLogLevel,
	cp platformorchestratorcp.ClientWithResponsesInterface,
	db model.Databaser, tx model.Tx,
) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, *DeploymentDiff, error) {
	if env.Status == platformorchestratorcp.EnvironmentStatusDeleting && mode != model.DeploymentModeDestroy {
		return nil, nil, nil, model.NewErrConflict(fmt.Sprintf("environment is in status '%s'", env.Status))
	}
	isRollback := mode == model.DeploymentModeRollback || mode == model.DeploymentModeRollbackPlan
	useLastGraph := mode == model.DeploymentModeDestroy || isRollback
	if useLastGraph && (len(manifest.Workloads) > 0 || len(manifest.Shared) > 0) {
		return nil, nil, nil, errors.Errorf("manifest should be empty on destroy or rollback deployments")
	}

	if env.RunnerId == nil {
		if r, err := cp.UpdateRunnerInAnEnvironmentWithResponse(ctx, orgId, projectId, envId, &platformorchestratorcp.UpdateRunnerInAnEnvironmentParams{}); err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to update runner in environment")
		} else if r.StatusCode() == http.StatusConflict {
			return nil, nil, nil, model.NewErrConflict(r.JSON409.Message)
		} else if r.StatusCode() != http.StatusOK {
			return nil, nil, nil, errors.Errorf("unexpected status code when updating runner in environment: %s: %s", r.Status(), string(r.Body))
		} else {
			env.RunnerId = ref.Ref(r.JSON200.RunnerId)
		}
	}

	// If the idempotency key was provided, then we use the hex(sha256(.)) digest.
	var idempotencyKeyDigest opt.Opt[string]
	if idempotencyKey != nil {
		h := sha256.New()
		_, _ = h.Write([]byte(*idempotencyKey))
		idempotencyKeyDigest = opt.Of(hex.EncodeToString(h.Sum(nil)))

		if d, m, err := db.GetDeploymentByIdempotencyKeyDigest(ctx, tx, orgId, projectId, envId, idempotencyKeyDigest.Must()); err != nil {
			if _, ok := model.IsErrNotFound(err); !ok {
				return nil, nil, nil, errors.Wrap(err, "failed to get deployment by idempotency key digest")
			}
		} else {
			var otherManifest DeploymentManifest
			if err := json.Unmarshal(m, &otherManifest); err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to unmarshal deployment manifest")
			} else if d.Mode != mode || (mode != model.DeploymentModeDestroy && !reflect.DeepEqual(otherManifest, manifest)) {
				return nil, nil, nil, model.NewErrConflict("incorrect manifest or mode for this idempotency key")
			} else {
				return d, nil, nil, nil
			}
		}
	}

	var lastDeploymentId uuid.UUID
	var lastGraph *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]
	var lastManifest DeploymentManifest
	var lastDeploymentMayHaveNotConverged bool
	{
		var d *model.DeploymentSummary
		var m model.EncodedDeploymentManifest
		var g model.EncodedDeploymentGraph
		var err error

		// First, look up the previous graph if it exists. This is either the previous deployment graph or the rollback target
		// graph, and we're going to grab the pinned versions from here.
		if rollbackTo.IsSet() {
			d, m, _, g, err = db.GetDeployment(ctx, tx, orgId, rollbackTo.Must(), model.GetModeDefault)
		} else {
			d, m, _, g, err = db.GetLastDeployment(ctx, tx, orgId, projectId, envId, model.GetLastDeploymentParams{StateChangeOnly: true})
		}

		if err != nil {
			// The first time we deploy an environment, there won't be a "last" deployment and we'll get a 404 here.
			// so this is normal, expected, and we can blank out the error.
			if _, ok := model.IsErrNotFound(err); !ok || rollbackTo.IsSet() {
				return nil, nil, nil, errors.Wrap(err, "failed to find reference deployment")
			}
			// or ignore
		} else if d.DeploymentEnvUuid != env.Uuid {
			return nil, nil, nil, model.NewErrConflict("cannot reference a deployment that is not part of this environment")
		} else if lastGraph, err = graphs.FromJson(bytes.NewReader(g)); err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to parse reference deployment graph")
		} else if err = json.Unmarshal(m, &lastManifest); err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to parse reference deployment manifest")
		} else if d != nil {
			lastDeploymentId = d.Id
			if d.Status == model.DeploymentStatusFailed {
				lastDeploymentMayHaveNotConverged = true
			}
			logger.Info("identified reference deployment", zap.String("deployment_id", d.Id.String()))
		}
	}

	if mode == model.DeploymentModeDestroy && lastGraph == nil {
		return nil, nil, nil, model.NewErrConflict("cannot destroy an environment that has never been deployed")
	}

	var runner platformorchestratorcp.Runner
	if r, err := cp.GetRunnerWithResponse(ctx, orgId, *env.RunnerId); err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to get runner")
	} else if r.StatusCode() == http.StatusNotFound {
		return nil, nil, nil, model.NewErrConflict(fmt.Sprintf("no runner exists with id '%s', the runner must be recreated or the environment must be updated with a new runner", *env.RunnerId))
	} else if r.StatusCode() != http.StatusOK {
		return nil, nil, nil, errors.Errorf("unexpected status code when getting runner: %s: %s", r.Status(), string(r.Body))
	} else {
		runner = *r.JSON200
	}

	// If the last graph was defined then the environment was deployed previously and may have pinned definitions that
	// we need to request for the next deployment.
	var pinnedDefinitions []string
	if lastGraph != nil {
		pinnedDefinitions = graphs.FindPinnedDefinitions(lastGraph)
	}

	// Now we can look up all our definitions, rules, and providers from the control plane.
	var moduleDefinitions []platformorchestratorcp.InternalModuleCatalogueModule
	var moduleProviders []platformorchestratorcp.ModuleProvider
	if r, err := cp.GenerateInternalModuleCatalogueWithResponse(
		ctx, orgId, projectId, envId,
		platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
			PinnedModuleVersions: pinnedDefinitions,
			AreRulesIgnored:      useLastGraph,
		},
	); err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to generate internal module catalogue")
	} else if r.StatusCode() == http.StatusNotFound {
		return nil, nil, nil, model.NewErrConflict(r.JSON404.Message)
	} else if r.StatusCode() == http.StatusConflict {
		if r.JSON409.Error == string(cperrors.PinnedModuleMissingProvider) {
			var details string
			if missingProviders, ok := ref.DerefOr(r.JSON409.Details, nil)["missing_providers"].([]interface{}); ok {
				details = fmt.Sprintf(": %s", missingProviders)
			}
			return nil, nil, nil, model.NewErrConflict(fmt.Sprintf("%s: existing module(s) depend on providers that no longer exist%s", r.JSON409.Error, details))
		}
		return nil, nil, nil, model.NewErrConflict(r.JSON409.Message)
	} else if r.StatusCode() != http.StatusOK {
		return nil, nil, nil, errors.Errorf("failed to generate internal module catalogue: %s: %s", r.Status(), string(r.Body))
	} else {
		moduleDefinitions = r.JSON200.Modules
		moduleProviders = r.JSON200.Providers
	}

	var newGraph *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]
	var err error
	if useLastGraph && lastGraph != nil {
		// When doing a destroy or a rollback, we use the same manifest and graph from the previous deployment
		// with pinned module definitions. This means that we should generate the same/similar terraform. The upside is that
		// all module elements and providers are present. The downside is that if a module is invalid, or not
		// accessible - the runner will fail and the user will need to do a normal deploy to update the module
		// definitions to ones that aren't broken.
		manifest = lastManifest
		newGraph = lastGraph
		_ = graphs.AssignModuleDefinitionsToGraphNodes(lastGraph, moduleDefinitions)
	} else {
		var g *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]
		g, err = graphs.BuildGraph(ctx, convertDpManifestToGraphManifest(manifest), moduleDefinitions, lastGraph)
		if err != nil {
			if e := new(platformorchestratorgraph.ErrGraph); errors.As(err, &e) {
				logger.Warn("failed to build graph", zap.Error(err))
				// TODO: We can do a whole load more interesting things here to reprocess this message and make it more useful
				//   to the developers. For example, stripping punctuation, differentiating between failures in the nodes
				//   the developer requested vs failures inside the modules themselves. etc.
				return nil, nil, nil, model.NewErrBadRequest(e.Error())
			} else if e := graphs.UserBadRequestError(""); errors.As(err, &e) {
				logger.Warn("failed to build graph", zap.Error(err))
				return nil, nil, nil, model.NewErrBadRequest(e.Error())
			}
			return nil, nil, nil, err
		}
		newGraph = g

		graphs.AddDeletedNodes(newGraph, lastGraph, lastDeploymentMayHaveNotConverged)
	}

	// Build the distance matrix once for use by both AddProviderMappingToNodes and BuildTofuFromGraph
	distanceMatrix := graphs.BuildGraphDistanceMatrix(newGraph)

	if err = graphs.AddProviderMappingToNodes(newGraph, moduleProviders, distanceMatrix); err != nil {
		if e := graphs.UserBadRequestError(""); errors.As(err, &e) {
			logger.Warn("failed to add provider mappings to graph", zap.Error(err))
			return nil, nil, nil, model.NewErrBadRequest(e.Error())
		}
		return nil, nil, nil, err
	}

	rawManifest, _ := json.Marshal(manifest)

	rawNewGraph, err := graphs.ToJson(newGraph)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to serialize graph")
	}

	contextMap := map[string]interface{}{
		"org_id":      orgId,
		"project_id":  projectId,
		"env_id":      envId,
		"env_type_id": env.EnvTypeId,
	}

	rawTofu, err := graphs.BuildTofuFromGraph(newGraph, moduleProviders, runner, env.Uuid, graphs.ContextPlaceholderLookupMap(contextMap), distanceMatrix)
	if err != nil {
		if e := graphs.UserBadRequestError(""); errors.As(err, &e) {
			logger.Warn("failed to build tofu", zap.Error(err))
			return nil, nil, nil, model.NewErrBadRequest(e.Error())
		}
		return nil, nil, nil, err
	}

	metrics := model.DeploymentMetrics{}
	if mode != model.DeploymentModeDestroy {
		metrics.Workloads = len(manifest.Workloads)
		metrics.ResourceNodes = len(newGraph.Nodes) - metrics.Workloads
	}

	var diff DeploymentDiff
	if lastGraph != nil {
		diff = DiffGraphs(env.Uuid, lastGraph, newGraph)
		diff.FromDeploymentId = &lastDeploymentId
	} else {
		diff = DiffGraphs(env.Uuid, &platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]{}, newGraph)
	}

	if dryRun {
		return &model.DeploymentSummary{
			OrgId:                     orgId,
			ProjectId:                 projectId,
			EnvId:                     envId,
			DeploymentEnvUuid:         env.Uuid,
			Mode:                      mode,
			RollbackToId:              rollbackTo,
			CreatedBy:                 createdBy,
			Status:                    model.DeploymentStatusExecuting,
			StatusMessage:             "dry run",
			RunnerId:                  runner.Id,
			Metrics:                   metrics,
			EncryptedOutputsRecipient: opt.OfRef(encryptedOutputsRecipient),
			EncryptedLogsRecipient:    opt.OfRef(encryptedLogsRecipient),
			RunnerLogLevel:            string(ref.DerefOr(runnerLogLevel, DeploymentCreateBodyRunnerLogLevelInfo)),
		}, nil, &diff, nil
	}

	d, err := db.CreateDeployment(ctx, tx, orgId, projectId, envId, model.CreateDeploymentParams{
		CreatedBy:                 createdBy,
		DeploymentEnvUuid:         env.Uuid,
		Mode:                      mode,
		Manifest:                  rawManifest,
		RollbackToId:              rollbackTo,
		Graph:                     rawNewGraph,
		Tofu:                      rawTofu,
		IdempotencyKeyDigest:      idempotencyKeyDigest,
		RunnerId:                  runner.Id,
		EncryptedOutputsRecipient: opt.OfRef(encryptedOutputsRecipient),
		EncryptedLogsRecipient:    opt.OfRef(encryptedLogsRecipient),
		Metrics:                   metrics,
		RunnerLogLevel:            string(ref.DerefOr(runnerLogLevel, DeploymentCreateBodyRunnerLogLevelInfo)),
	})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to create deployment")
	}
	diff.ToDeploymentId = ref.Ref(d.Id)

	if d.Mode != model.DeploymentModeDeployPlan {
		arGraph := newGraph
		if d.Mode == model.DeploymentModeDestroy {
			arGraph = new(platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig])
		}
		if err := db.InitActiveResourcesFromGraph(ctx, tx, d.DeploymentEnvUuid, d.Id, arGraph); err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to init active resources from graph")
		}
	}

	msg := &hstandardoutbox.PendingEventMessage{
		Subject: string(genevents.IoPlatformOrchestratorDeploymentCreated),
		Payload: model.ConvertDeploymentToEventPayload(d),
	}
	messages, err := db.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{msg})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to insert pending event messages")
	}

	logger = logger.With(logging.ZapDeploymentId(d.Id.String()), logging.ZapRunnerId(d.RunnerId))
	logger.Info(
		"deployment created",
		zap.String("mode", string(d.Mode)),
		zap.Int("rawManifestBytes", len(rawManifest)),
		zap.Int("rawGraphBytes", len(rawNewGraph)),
		zap.Int("rawTofuBytes", len(rawTofu)),
		zap.Int("numDefinitions", len(moduleDefinitions)),
		zap.Int("numProviders", len(moduleProviders)),
	)

	return d, messages, &diff, nil
}

func formatResourceCoordinate(rc platformorchestratorgraph.ResourceCoordinate) string {
	return fmt.Sprintf("%s.%s@%s", rc.Type, rc.Class, rc.Id)
}

// DiffGraphs produces a human-readable diff of the two graphs with nodes added, removed, or changed.
func DiffGraphs(envUuid uuid.UUID, before, after *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]) DeploymentDiff {
	changes := make([]DeploymentDiffChange, 0)

	for rc := range after.DepthFirstIterate(platformorchestratorgraph.DepthFirstIteratePreOrder) {
		newNode := after.Nodes[rc]
		if newNode.ModuleConfiguration != nil && newNode.ModuleConfiguration.Deleted {
			continue
		}
		if oldNode, ok := before.Nodes[rc]; ok {
			// If the node module configuration
			if newNode.ModuleConfiguration == nil {
				if !reflect.DeepEqual(newNode.Params, oldNode.Params) {
					changes = append(changes, DeploymentDiffChange{
						Id:       util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id),
						Resource: formatResourceCoordinate(rc),
						Type:     DeploymentDiffChangeTypeParamsChanged,
						Summary:  "workload output variables changed",
					})
				}
				continue
			}
			if oldNode.ModuleConfiguration.DefinitionId != newNode.ModuleConfiguration.DefinitionId || oldNode.ModuleConfiguration.VersionId != newNode.ModuleConfiguration.VersionId {
				changes = append(changes, DeploymentDiffChange{
					Id:       util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id),
					Resource: formatResourceCoordinate(rc),
					Type:     DeploymentDiffChangeTypeModuleChanged,
					Summary:  fmt.Sprintf("module changed from %s@%s to %s@%s", oldNode.ModuleConfiguration.DefinitionId, oldNode.ModuleConfiguration.VersionId, newNode.ModuleConfiguration.DefinitionId, newNode.ModuleConfiguration.VersionId),
				})
			} else if newNode.Params != nil && !reflect.DeepEqual(newNode.Params, oldNode.Params) {
				summary := "resource params changed"
				if newNode.ParamsDefinedBy != nil {
					summary = fmt.Sprintf("resource params changed by %s", formatResourceCoordinate(*newNode.ParamsDefinedBy))
				}
				changes = append(changes, DeploymentDiffChange{
					Id:       util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id),
					Resource: formatResourceCoordinate(rc),
					Type:     DeploymentDiffChangeTypeParamsChanged,
					Summary:  summary,
				})
			}
		} else {
			summary := "workload added"
			if newNode.ModuleConfiguration != nil {
				summary = fmt.Sprintf("add resource using module %s@%s", newNode.ModuleConfiguration.DefinitionId, newNode.ModuleConfiguration.VersionId)
				if newNode.ParamsDefinedBy != nil {
					summary += fmt.Sprintf(" (dependency of %s)", formatResourceCoordinate(*newNode.ParamsDefinedBy))
				}
			}
			changes = append(changes, DeploymentDiffChange{
				Id:       util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id),
				Resource: formatResourceCoordinate(rc),
				Type:     DeploymentDiffChangeTypeAdded,
				Summary:  summary,
			})
		}
	}

	for rc := range before.DepthFirstIterate(platformorchestratorgraph.DepthFirstIteratePreOrder) {
		oldNode := before.Nodes[rc]
		if n, ok := after.Nodes[rc]; ok && (n.ModuleConfiguration == nil || !n.ModuleConfiguration.Deleted) {
			continue
		}
		summary := "workload removed"
		if oldNode.ModuleConfiguration != nil {
			summary = fmt.Sprintf("remove resource using module %s@%s", oldNode.ModuleConfiguration.DefinitionId, oldNode.ModuleConfiguration.VersionId)
			if oldNode.ParamsDefinedBy != nil {
				summary += fmt.Sprintf(" (dependency of %s)", formatResourceCoordinate(*oldNode.ParamsDefinedBy))
			}
		}
		changes = append(changes, DeploymentDiffChange{
			Id:       util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id),
			Resource: formatResourceCoordinate(rc),
			Type:     DeploymentDiffChangeTypeRemoved,
			Summary:  summary,
		})
	}
	var added, removed int
	for _, change := range changes {
		switch change.Type {
		case DeploymentDiffChangeTypeAdded:
			added++
		case DeploymentDiffChangeTypeRemoved:
			removed++
		}
	}
	return DeploymentDiff{Changes: changes, NumAdded: added, NumRemoved: removed, NumChanged: len(changes) - added - removed}
}

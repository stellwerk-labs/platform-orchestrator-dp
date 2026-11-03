# Optional Environment observations and Runner job capacity

`environment-observation/v1` is an additive protocol. Its canonical schema is
`openapi/observation.yaml` and its optional generated Go client is
`shared/v3/observationapi`. Existing v3 mandatory client/server interfaces and
Rollouts operations are unchanged. Runner completion adds the optional
`observation_result` field to the existing result body.

The signed CP-to-DP boundary uses the existing operator-configured verification
key, issuer and `platform-orchestrator-dp` audience. Every assertion binds exact
organization, plugin, actor, action, HTTP method/path/body and request identity.
Core holds the authoritative plugin/grant fencing transaction over try-start and
rechecks current Admin membership, entitlement, enablement and exact grant scope.
The plugin never receives deployment manifests, generated configuration, state,
raw plans, diagnostics, outputs or direct database/broker credentials.

| Route below `/orgs/{orgId}/plugins/{pluginId}/observations/v1` | Signed action |
| --- | --- |
| `POST /runs` | `observation.try-start` |
| `GET /runs/{runId}` | `observation.get` |
| `GET /requests/{requestId}` | `observation.request.get` |
| `POST /requests/{requestId}/resolve` | `observation.request.resolve` |
| `POST /runs/{runId}/redeploy` | `observation.redeploy` |
| `GET /redeployments/{requestId}` | `observation.redeploy.get` |
| `POST /redeployments/{requestId}/resolve` | `observation.redeploy.resolve` |

A POST uses its UUID `request_id` as `Idempotency-Key`. An admission window is
positive and at most ten minutes from the UTC occurrence. First delivery after
its deadline seals an expired skip. Exact request replay returns the original
accepted/skip receipt, even if Runner configuration or the Environment changed.
Changed content or actor conflicts. Read ambiguous delivery through the request
receipt endpoint; a missing receipt alone does not prove that work never started.
The resolve endpoint accepts the original request, atomically returns its accepted
receipt or seals it as not started, and excludes later admission of that identity.
Never invent a fresh identity for an old occurrence.

## Admission and immutable baseline

One PostgreSQL transaction holds the request identity and the same Environment
advisory lock used by ordinary creation, enqueue and promotion. Every creation
mode, including ordinary plans, takes that lock. An observation is admitted only
with no running work, no waiting queue item, no unreconciled observation outcome
and an available persistent Runner slot. Busy, full, unsupported and unavailable
baseline outcomes seal skips. They create no Deployment, lease, queue item,
Runner reservation or outbox event. Generic readiness also reports running
plans/observations as busy and uses the same failed-observation ownership fence
as creation. It cannot report ready until authoritative stop clears that fence.

The current mutating head must be a completed successful deploy or rollback,
including one initiated by another plugin. Draft tests, destroy, plan and
observation runs cannot be baselines. A newer failed or unknown mutation cannot
be skipped to select an older success. An older failed ordinary attempt that
still owns a Runner reservation also blocks observation after a newer success,
until authoritative stop is retained. Missing complete retained configuration
or immutable Module/artifact identity is unavailable. Observations copy exact
retained manifest, graph, generated Terraform/OpenTofu and Module artifact
identities. Inline material retrieval checks the retained UUID/digest/source;
it never recompiles with current defaults or substitutes another version.

Core artifact digests remain optional. Retained immutable inline source and
executable Git source URLs pinned to a full commit qualify without one. A
descriptive revision field alone does not freeze a mutable source URL. Supplied
digests must still match retained catalogue metadata; source kinds without
immutable selectors or a retained digest cannot establish this baseline.

An admitted Deployment has explicit internal mode `observation`, and sends
`MODE=plan_only` plus `OBSERVATION_PROTOCOL=environment-observation/v1` to Runner. It updates the executing head, retains the normal
Environment lease and uses the standard created-event/outbox/bundle/dispatch
path. It never updates the state head, active resource metadata or adoption.
An unavailable retained bundle fails safely before dispatch. Preparation cannot
start a previously unsent observation after the one-hour execution budget.

## Results, freshness and retention

Only authenticated complete Runner results can establish successful observation
outcomes. The safe v1 result contains tool/version, observed changes, proposed
actions and an output-change count. Changes contain opaque IDs, resource types
and action enums. Addresses and attribute/output values are absent. Each change
array is bounded to 2,000 items, and retained JSON is at most 1 MiB. Missing,
unsupported, incomplete or invalid results cannot become clean. Two-action
changes are accepted only for create/delete replacement, in either order.

Safe results expire after 30 days. A separate bounded worker removes expired
result JSON, by default up to 500 rows every five seconds. Ordinary deployment
cleanup is unchanged. Durable immutable receipt/idempotency tombstones remain available;
expired or deleted run data returns `expired`, never an empty clean report.
A successful retained observation projects `stale` if a newer successful
mutating head replaced its captured baseline. A failed, running or unknown
current mutation projects `unavailable`. These projections retain historical
safe results and never rewrite the stored observation terminal outcome.

An observation timeout does not prove that its Runner stopped. Its Runner slot
and Environment fence remain held until authenticated completion or an audited
operator stop attestation. Exact late observation completion releases ownership
without changing an already failed observation into success. Ordinary late
completion similarly retains only a neutral payload digest and stop proof,
without rewriting failure, outputs, resources or adoption. Existing external
plugin recovery lineage rules remain unchanged.

## Shared Runner capacity

`max_parallel_jobs` is optional and positive on every Runner configuration.
Omission preserves unlimited legacy admission. Observation additionally requires
explicit `supported_protocols: [environment-observation/v1]` and a configured
limit. An old Runner therefore receives its known read-only plan mode. A new Runner
requires the exact protocol marker to enter its sanitized observation path.
Missing structured output from an old Runner cannot become clean. The actual
published stable Runner rejects unknown mode during configuration validation;
using its known plan mode is additional compatibility defense. Final Kubernetes
Pod template patches and ECS environment/secrets cannot override protected
observation mode/protocol or replace the standard Runner entrypoint.

Persistent reservations are keyed by organization and Runner, and are claimed
before every standard Runner dispatch, including direct Kubernetes, Kubernetes
agent and ECS. NATS retry and process restart reuse the same reservation.
Ordinary jobs wait in the existing durable dispatch path when full. Due
observations skip atomically. Lowering a limit lets already reserved jobs drain;
no new job claims a slot while usage is at or above the limit.

Reservations have no TTL. Authenticated completion releases them once. A failure
before any dispatch can release them because no command was sent. A watchdog
failure, a broker timeout or a closed execution deadline does not prove stop.
Migration 38 conservatively preserves potentially live executing or uncertain
failed older attempts. Legacy authenticated success is positive completion
evidence even when old dispatch-completion markers are absent. Opting a
historical Runner into a limit may therefore require reconciling old unknown
jobs first. The default remains unlimited.

The existing attributable-user endpoint
`POST /internal/orgs/{orgId}/deployments/{deploymentId}/actions/acknowledge-outcome`
also accepts failed jobs with retained Runner ownership after their immutable
dispatch budget closes. It requires `deployment.outcome.acknowledge`, an exact
reason and actor-stamped audit. It does not stop a job, change its status or
approve infrastructure changes. The operator must first establish authoritative
stop. Ordinary capacity attestations release only that reservation. Existing
plugin/observation fence acknowledgements reject waiting work as before.
Even forced Environment deletion cannot erase live Runner ownership. Released
reservations can be removed during normal Environment deletion; receipt
identities survive as tombstones. Force skips new Destroy work; it never stops or
forgets a possibly live job. Deletion handlers re-read CP's canonical deletion
policy for the exact Environment UUID on every wake. Force and delete-rules
choices survive ordinary completion, redelivery and restart. They can escalate to
true within one Environment UUID; a later default/false retry cannot undo them.
A legacy deleting
Environment whose policy is unrecorded requires an explicit repeated deletion
request. A newly recorded acknowledgement durably wakes cleanup in the same
transaction, while an identical replay does not enqueue another wake.

## Explicit remedial deployment

Redeploy requires a fresh delegated user and ordinary Core deployment admission,
including current IAM, Pin/lifecycle, Environment-busy and baseline checks.
The captured run/baseline fingerprint must still match the current successful
mutating head. DP replays exact retained configuration through normal deploy,
active-resource and outbox paths. It never applies the observation's saved plan.
An independent durable receipt resolves ambiguous redeploy submission. Its GET
returns the actual ordinary deployment status, or `unavailable` if that data is
no longer retained. Clicking Deploy does not establish that drift was corrected.

## Upgrade and forward recovery

Migration 38 is additive and restart safe. It backfills persistent potentially
live Runner ownership without inventing completion. Down refuses observation
history, live ownership or retained late-completion evidence. Older binaries
already reject a schema version newer than their embedded migration set.
Use a compatible forward recovery, keep plugin-disabled operation available,
and preserve database/broker evidence. Do not reset statuses or remove
reservations to work around an uncertain Runner.

The locking and plan choices follow the existing queue contract and
[PostgreSQL transaction advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS),
[Terraform normal plan behavior](https://developer.hashicorp.com/terraform/cli/commands/plan)
and its [versioned JSON format](https://developer.hashicorp.com/terraform/internals/json-format).

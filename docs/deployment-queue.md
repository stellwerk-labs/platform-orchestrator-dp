# Environment deployment queue (schemas 33 to 35)

Deploy and rollback Deployments change their Environment and therefore run one
at a time. Until schema 33, a request that found its Environment busy failed
with 409 and the client had to retry. The deployment queue lets the request
wait instead: it is stored as a queue item and becomes a Deployment when the
Environment is free and the item is at the head of the Environment's queue.

Waiting is the default. A deploy or rollback request without `when_busy`, or
with `when_busy: queue`, that finds its Environment busy is stored and answered
with 202 and the item. Only `when_busy: reject` keeps the fail-fast answer: 409
with the error code `environment_busy`. Requests authorized by an organization
token cannot wait (see below) and get that 409 either way. Plan-only requests
and dry runs are never queued.

## What is stored

A queue item stores the request as the client sent it: manifest, rollback
target, explicit Module Versions, restricted-version confirmations, output and
log recipients, Runner log level, the idempotency key digest, an optional
`expected_base_deployment_id` and `halt_on_failure`. It never stores a resolved
graph.

The item is resolved like a new request when it is promoted, as the requesting
user and against the Environment as it is then: base graph, Pins, catalogue
defaults, Module lifecycle, Runner and Core admission. A snapshot resolved at
enqueue would be based on a head the queued predecessor replaces, silently
reverting what the predecessor deployed. The consequence is that an item can
be rejected later, for example when a Pin was created or a Version marked
Defective in the meantime. Clients that derive a manifest from a specific
Deployment (merge flows, a diff confirmed at a prompt) send
`expected_base_deployment_id`; creation then fails with
`deployment_base_changed` when the Environment's current Deployment differs.
The check applies to direct requests and promoted items alike, not to dry runs.

## States

```text
 enqueue --> [queued] --cancel--------> [cancelled]   actor and reason recorded
               |  \----TTL / deletion--> [expired] / [cancelled]
               +--promote--> [dispatched]  deployment_id; the Deployment owns everything after this
               +--promote--> [rejected]    permanent failure or halt_on_failure, reason_code and reason_message
```

Only `queued` items change. Every transition is a compare-and-set on
`state = 'queued'`, so an item settles exactly once.

A cancelled, expired or rejected item records why in `reason_code` and
`reason_message`. When the reason refers to a Deployment,
`reason_deployment_id` names it, so clients never parse the message:

| `reason_code` | `reason_deployment_id` |
|---|---|
| `previous_deployment_failed` | the failed Deployment the item waited for |
| `outcome_unreconciled` | the failed plugin-dispatched Deployment that fences the Environment |
| `deployment_base_changed` | the Environment's current Deployment, which replaced the expected base (absent when the Environment has none) |
| any other | absent |

A dispatched item has no reason; `deployment_id` names the Deployment it
became.

## Request handling

`POST /orgs/{orgId}/deployments` for a deploy or rollback, unless it sets
`when_busy: reject`:

1. Validation and permissions are unchanged. `queue_ttl_seconds` and
   `halt_on_failure: true` are refused (400) together with `when_busy: reject`;
   `queue_ttl_seconds` must not exceed the configured maximum. Plan-only
   requests and dry runs are never queued and ignore all three fields (see
   "Plan-only requests and dry runs").
2. A keyed request that matches a waiting, cancelled, expired or rejected item
   returns that item (202); a different request on the same key returns 409.
   When a Deployment carries the key, because the item was dispatched or the
   request was created at once, that Deployment's own idempotent replay
   answers, also while it still runs and keeps the Environment busy.
3. A deleting Environment answers 409 `environment_deleting`. An Environment
   fenced by an unreconciled plugin-dispatched Deployment answers 409
   `outcome_unreconciled`.
4. If the Environment is busy (lease, an executing Deployment including a plan
   run, or waiting items), the request is enqueued without contacting Core.
   Otherwise it is created directly and answers 201 as before. If that direct
   attempt finds the Environment busy after all, including Core still guarding
   the previous admission, it is enqueued instead.
5. Enqueueing takes the Environment lock, re-checks the fence, enforces the
   depth limit (409 `deployment_queue_full`) and inserts the item with
   `expires_at = now() + TTL`.

A request authorized by a deprecated organization token is never queued: there
is no user whose authority promotion could re-check. Whether it omits
`when_busy` or sets `queue`, a busy Environment answers 409 `environment_busy`
with the message "the Environment is busy; queueing a Deployment requires an
attributable user, not an organization token"; with `reject` it gets the
ordinary busy message.

Queue endpoints:

| Endpoint | Permission |
|---|---|
| `GET /orgs/{orgId}/deployment-queue?project_id&env_id&state` (FIFO order, positions) | `deployment_read` on the organization |
| `GET /orgs/{orgId}/deployment-queue/{queueItemId}` (with the requested manifest) | `deployment_read` on the organization |
| `POST /orgs/{orgId}/deployment-queue/{queueItemId}/actions/cancel` `{"reason"}` | `deployment_write` on the Environment |

Any writer of the Environment may cancel obsolete work. Repeating the identical
cancellation returns the cancelled item; another cancellation of a settled item
answers 409 `queue_item_settled`. A cancellation that meets a promotion in
progress answers 409 `queue_item_dispatching`; the caller reads the item again.

## Plan-only requests and dry runs

Plan-only requests (`mode: plan_only` or `plan_only: true`, also rollback
plans) and dry runs (`is_dry_run: true`) change nothing. They are never queued
and ignore `when_busy`, `queue_ttl_seconds` and `halt_on_failure`, so a client
may send its usual queue settings with them.

- A dry run resolves the request and answers 200 with the diff, also while a
  Deployment runs or requests wait. It takes no lease, no Core admission and no
  queue position, and it never starts a Runner.
- A plan-only request creates a Deployment that runs a Runner in plan mode. It
  takes no lease and no queue position. It starts while requests wait in the
  queue, and the head then waits for the plan run to finish. It does not start
  while a deploy, rollback or destroy runs (or another plan run): that answers
  409 `environment_busy`, whatever `when_busy` says. See "State lock" below.

Both answers carry `pending_changes` when Deployments will change the
Environment before anything requested now:

```json
"pending_changes": {"code": "result_may_change", "running": 1, "queued": 2}
```

`running` counts the deploy, rollback or destroy that changes the Environment
now (0 or 1), `queued` the requests waiting in its queue. The plan or diff
describes the Environment as it is now, so it may still change. The field is
absent when neither runs or waits, and it only appears on the response that
created the plan-only Deployment or answered the dry run, never on later reads.

### State lock

Every mode of an Environment uses the same Terraform/OpenTofu state: the
backend block that `internal/graphs/tofu.go` generates is keyed by the
Environment UUID (Kubernetes `secret_suffix`, S3 `key`, GCS `prefix`, AzureRM
`key`). The Runner (v3.1.0) runs `init`, then `plan -out plan.tfplan` in every
mode, then `apply plan.tfplan` in deploy mode, without `-lock=false` or
`-lock-timeout`. Each step therefore takes the backend's state lock and fails
at once when another process holds it (Kubernetes Lease, GCS lock file, AzureRM
blob lease; the S3 backend as generated has no lock at all). A plan run beside
a deploy would make whichever step locks second fail, including the deploy's
own plan or apply. The Data Plane therefore keeps plan runs apart from every
other Runner of the Environment: a running plan counts as busy for the queue,
and a plan does not start while another Runner runs. A plan that runs beside a
deploy needs the Runner to plan without the lock (`plan -lock=false` in
plan-only mode; a plan persists no state) and to wait briefly for the lock in
the other modes (`-lock-timeout`), because the Kubernetes, GCS and AzureRM
backends still lock once to create an empty state for an Environment that has
none.

## Promotion

Any Data Plane replica promotes; there is no leader. One transaction holds:

```text
head row   SELECT ... FOR UPDATE SKIP LOCKED   concurrent promoters skip, cancellation gets NOWAIT
env lock   pg_advisory_xact_lock               taken by CreateDeployment; serialises every creation path
lease      environment_deployment_leases       unchanged; acquired by CreateDeployment
state CAS  UPDATE ... WHERE state = 'queued'
```

The head is the oldest `queued` item of the Environment. The claim selects that
row by identity and skips it when another promoter holds it, so a second
promoter never jumps to the next item. Inside the claim:

1. An expired head is settled `expired` and the next item is tried.
2. If the Environment is busy, nothing happens; the event that frees it
   triggers promotion again. If it is fenced by an unreconciled
   plugin-dispatched Deployment, every waiting item is rejected
   (`outcome_unreconciled`).
3. A `halt_on_failure` head whose predecessor failed is settled `rejected` with
   `previous_deployment_failed` and the next item is tried at once, without
   contacting Core (see "Halt on failure").
4. The Environment is read from Core, the requester's `deployment_write`
   permission is checked again, and the stored request is created through the
   unchanged Deployment path with a fresh Core admission attempt, inside a
   savepoint. `CreateDeployment` lets only this head pass the queue check.
5. Success settles the item `dispatched` with the new Deployment in the same
   transaction; the `deployment.created` outbox message follows the commit.
6. Failure rolls back to the savepoint, seals the Core admission absent and
   classifies the error:

| Class | Errors | Effect |
|---|---|---|
| transient | lease or executing Deployment, Core `environment_busy` (an earlier admission or plugin dispatch is unresolved), Core or IAM unavailable, any untyped error | item stays queued, `attempts`, `last_error`, `next_attempt_at` with backoff from 5 s doubling to 2 min, bounded by the TTL |
| permanent | invalid request, `actor_forbidden`, `environment_deleting`, `environment_not_found` (also when the Environment was recreated), missing rollback target (`not_found`), Core denial with its code (`pin_protection`, `module_version_lifecycle`), `deployment_base_changed`, other conflicts | item `rejected` with reason; the next item is tried at once |
| fence | `outcome_unreconciled` | every waiting item of the Environment is rejected |

A failed predecessor does not stop the queue: an ordinary failed Deployment
never blocks the next one, unless that item asked to halt.

### Halt on failure

A request may set `halt_on_failure: true` (default `false`). The item is then
not promoted when the Deployment it waited for failed; it ends `rejected` with
the reason code `previous_deployment_failed` and that Deployment in
`reason_deployment_id` (and in the message), and the queue continues with the
next item. Only the item that asked halts; items without the flag behave as
before.

The predecessor is the Environment's latest deploy, rollback or destroy
Deployment when the item reaches the head of the queue. Those run strictly one
after another, so it is the Deployment that ran immediately before the item,
whether it came from an earlier queue item, a direct request or a plugin
dispatch. It counts only when the item waited for it, that is when it had not
ended yet when the item was enqueued:

| Situation at promotion | Result |
|---|---|
| Predecessor ended failed after the item was enqueued | `rejected`, `previous_deployment_failed` |
| Predecessor succeeded | promoted |
| Predecessor had already ended when the item was enqueued (the item waited only for Core to settle it, for a plan run or for items ahead) | promoted, even if it failed |
| No deploy, rollback or destroy ever ran in the Environment | promoted |
| Several Deployments ran while the item waited | only the last counts: a later success supersedes an earlier failure, a later failure halts |
| Items ahead were cancelled, expired or rejected | they never became Deployments and are not predecessors; the item looks past them |
| Predecessor force-failed by an operator | it is `failed` and counts as a failure (Deployments have no cancelled state) |
| Predecessor is a failed plugin-dispatched Deployment with an unreconciled outcome | the fence wins: every waiting item is rejected `outcome_unreconciled`, as before |

Plan runs never count, neither as predecessor nor as "ran while waiting". One
failure halts every waiting item with the flag, because none of them can be
preceded by anything else. To decide "had not ended yet", enqueueing a
`halt_on_failure` item records the Environment's current Deployment in
`enqueued_after_deployment_id` when it had already ended, under the
Environment lock. Promotion checks the flag before
Core admission, so a halted item never reaches Core. A request that starts at
once never halts, because it waited for nothing.

### Triggers

- After an enqueue or a cancellation commits, and after a direct attempt was
  refused as busy (it may have held Core's admission guard while the head
  tried), the request handler starts a background promotion for the
  Environment. Promotions for one Environment are coalesced per process.
- The `deployment.updated` handler settles Core's admission guard and then
  promotes the Environment's queue when the Deployment reached a terminal
  status. Event triggers ignore the retry backoff, so a head that met Core's
  still-settling guard is promoted right after settlement.
- A sweeper on every replica runs every 15 s. It expires waiting items past
  their TTL (skipping items a promoter holds), prunes settled items after 30
  days, and promotes Environments whose head is due. It covers lost events,
  lease expiry and process restarts.

Background promotion starts from a fresh context. A request context carries
the echo context, whose organization-token shortcut must never authorize the
re-check of another user's queued request.

## Why this is safe with several replicas and process loss

- Two promoters cannot dispatch the same item: the head row lock admits one,
  `deployment_id` is unique and the state transition is a compare-and-set.
- No other creation path can overtake the queue: direct requests, plugin
  dispatches, captured-base recovery rollbacks and destroy all pass
  `CreateDeployment`, which under the Environment lock refuses with
  `environment_busy` while any item waits, unless it promotes that very head.
  Enqueueing takes the same lock, so the check cannot be raced.
- Only one Deployment runs: the lease and the executing-head check are
  unchanged.
- Nothing outside the database happens before commit except Core admission.
  Its receipt is durable before the remote call. If the process dies after
  Core admitted and before commit, the connection's transaction is rolled
  back, the item stays queued, `ReconcileUnsettled` seals the receipt absent,
  and the next attempt uses a new admission identity. Until the seal, Core
  answers the next attempt with `environment_busy`, a transient failure.
- Lock hold time is bounded by `lock_timeout` (5 s) and
  `idle_in_transaction_session_timeout` (60 s) on the promotion transaction.

## Plugin dispatches, readiness and the fence

Plugin-dispatched Deployments are never queued. While the lease is held or any
item waits, `CreateDeployment` refuses them and the external admission answers
409 `environment_busy`; external readiness reports the Environment `busy`
while items wait. Core itself refuses a plugin dispatch with `environment_busy`
while an ordinary admission is unresolved. A plugin that receives
`environment_busy` after Core recorded its dispatch must withdraw that
dispatch; until then Core keeps refusing the queue head's admission, which the
queue treats as transient.

An Environment fenced by a failed plugin-dispatched Deployment with an
unreconciled outcome accepts no new item (409 `outcome_unreconciled`). Items
that waited when the fence appeared are rejected by the next promotion, and at
the latest by `acknowledge-outcome`, which rejects them in its own transaction.
Acknowledging therefore never starts queued work: an operator incident is not
followed by an automatic burst of Deployments. New requests work normally
afterwards. The fence never applies to Core-only installations. Clients read
it as `environment_fence` on the Environment's `last-deployments` item (see
`runner-dispatch-recovery.md`).

## Deletion and destroy

When Core reports an Environment as deleting, the destroy handler first
cancels every waiting item (`environment_deleting`). Items enqueued in a race
afterwards are rejected at promotion because the Environment is deleting.
Destroy is never queued. It waits for an executing Deployment as before and,
new, treats a busy Environment at destroy creation (a lease taken in a race, a
plan run, Core still settling, a waiting item) as a reason to retry the event
after 10 s instead of setting the Environment to `delete_failed`. Deleting the
Environment's Deployment data also deletes its queue history.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DEPLOYMENT_QUEUE_MAX_DEPTH` | 10 | waiting items per Environment |
| `DEPLOYMENT_QUEUE_DEFAULT_TTL` | 2h | TTL without `queue_ttl_seconds`; longer than one lease so one full-length predecessor never expires the next item |
| `DEPLOYMENT_QUEUE_MAX_TTL` | 24h | upper bound of `queue_ttl_seconds`; must not be below the default |

## Upgrade and fallback

Schema 33 is additive. Its Down migration refuses to run while any item is
`queued`, so accepted work is never dropped silently; cancel or let waiting
items settle first. Schema 34 adds `halt_on_failure` (default `false`, so items
that waited during the upgrade keep going after a failure, as before) and
`enqueued_after_deployment_id`. Its Down migration refuses to run while a
`queued` item has `halt_on_failure`, because dropping the flag would let that
item start after a failure it asked to halt on. Schema 35 adds
`reason_deployment_id`; items settled before it have none and keep naming the
Deployment only in `reason_message`. Its Down migration drops the column
without a check, because only settled items carry it.

Behaviour change for clients that omit `when_busy`: a busy Environment now
answers 202 with a queue item instead of 409 `environment_busy`, and the
request runs later. Clients that must fail fast send `when_busy: reject`.
Plan-only requests and dry runs with `when_busy: queue` are no longer refused
with 400. Conflicts caused by Core admission keep their HTTP status and carry
Core's code when Core sends one.

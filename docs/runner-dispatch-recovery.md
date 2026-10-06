# Runner dispatch and completion recovery (schema 31)

The one-hour execution budget starts at the first durable dispatch attempt,
after the Runner bundle is prepared and before `Runner.Start` can have an
external effect. Time spent awaiting bundle construction does not consume a
new Deployment's execution budget. The timestamp is committed before dispatch;
restarts, broker redelivery and retries never refresh it. Remote command expiry
is capped at that same deadline. An expired or unknown budget cannot start a
Runner. Admission time remains unchanged historical evidence.

Migration 31 adds dispatch budgets and late Runner-result evidence. Existing
Deployments have no reliable first-dispatch timestamp, so their budget is
conservatively backfilled from their original UTC admission time, not refreshed.
The migration does not change their status, history, resources or outputs.

## Timeout is not proof that the Runner stopped

A possibly dispatched failed Deployment without authoritative completion is
unreconciled. Operator force-fail and Runner error messages do not prove
completion. A failure before any recorded dispatch is distinguishable from a
potentially started attempt.

Current authenticated HTTP results retain completion proof transactionally.
NATS deployment results use their exact organization/Runner/Deployment inbox
receipt. Legacy failed HTTP results have no durable completion evidence and
stay unreconciled. Late HTTP completion after an already terminal failure is
not recovered by the NATS evidence path below.

## Which Deployments fence their Environment

Only a failed **plugin-dispatched** Deployment with an unreconciled outcome
blocks its Environment. Plugin-dispatched means it was admitted through a
plugin operation receipt on the external-deployment API. While such a
Deployment is the Environment head, every new Deployment of that Environment
is rejected with a conflict, even after its lease expired.
External status inspection returns HTTP 409 `outcome_unreconciled` for it.

Ordinary Core Deployments keep their existing behaviour: a failed Deployment,
with or without Runner stop proof, never blocks the next Deployment of its
Environment. This includes legacy failures backfilled by migration 31, so an
upgraded installation stays deployable without data changes.

## Genuine late completion

An authenticated late NATS `deployment-result` may be retained only for the
exact current failed attempt, durable dispatch revision, owned operation
receipt, first-base anchor and complete successful captured base. Foreign
scope, a conflicting head or lease, conflicting second result, and error-only
events are rejected. Exact replay is idempotent, including after a subsequently
admitted recovery. The retained result contains neutral identity/status/time
and the canonical envelope digest, not the plaintext Runner body.

Late completion never rewrites the failed Deployment, its failure history,
resource metadata or adoption state into success. It proves the attempt has
stopped, allowing an explicitly authorized normal captured-base rollback with
the usual lease and current-head checks. No infrastructure retry or rollback
is started automatically. Successful late outputs retain the original
already-encrypted bytes and recipient. Protected material access still requires
the same scoped receipt and wrapped operation identity; the existing cleanup
ownership rules apply. Neutral completion evidence survives ciphertext cleanup.

## Upgrade and fallback

Schema 31 is additive. Its Down migration refuses to discard a new dispatch
attempt or any retained late completion. It is not safe to assume an older
schema-30 binary can run against this database or to downgrade after evidence
has been recorded. Qualify an approved schema-31-compatible forward-only
fallback, including the plugin-disabled path, before enabling a recovery.
Retain original deployment, broker and encrypted-material evidence. Do not
edit database statuses, duplicate an override, or blindly replay an uncertain
send. Transport redrive requires a separately reviewed, once-only operational
procedure and fresh stop/head/lease/ownership proof; this change supplies no
automatic redrive API.

# Read-only Environment observations, release candidate

This candidate adds bounded Environment observations for native plugins and
persistent Runner capacity accounting. Publication and the coordinated release
versions are still pending.

## Changes

- Admit an observation only when the Environment has no running or waiting
  work and its Runner has available configured capacity. Busy observations are
  skipped, leaving ordinary deployment queues authoritative.
- Capture the exact most recent successful ordinary deployment or rollback as
  the baseline, including ordinary deployments initiated by other plugins.
  A newer failed or uncertain mutation prevents using an older baseline.
- Execute a normal refreshed plan without applying it or persisting refreshed
  state. Retain bounded change IDs, resource types, actions and counts; raw
  plans, addresses, values and provider diagnostics are not observation results.
- Preserve durable observation and corrective-deployment receipts across lost
  acknowledgements and retries. Ambiguous work retains its reservation until an
  authoritative outcome establishes completion or no execution.
- Account for `max_parallel_jobs` across Runner job paths using persistent
  reservations. Omission preserves existing unlimited behavior.
- Accept retained immutable inline source or an external Git source URL pinned
  to a full commit without requiring an artifact digest. A supplied digest must
  still match; a descriptive revision label alone does not pin a mutable URL.

## Upgrade

Back up the affected databases and deployment state and use the qualified
coordinated CP, DP, IAM and Runner versions. Final version and image digest
references will be added after qualification. The new optional observation SDK
package must be published before downstream applications consume it.

Observations require a positive Runner capacity limit and an explicit
`environment-observation/v1` protocol declaration for a compatible executable.
An older Runner retains its plan-only behavior but cannot return a valid
structured observation result. Such a result is unavailable, never clean.

Ordinary deployments, rollbacks and destroys retain their normal behavior.
Plugin absence does not enroll Environments or schedule observations.

# Immutable Module Draft test execution

Unreleased candidate. Requires matching Control Plane shared contracts and API
routes; the local Go workspace is not a publishable SDK dependency pin.

Adds explicit immutable Draft test selection through the normal deployment
planner, queue, admission, encrypted artifacts and Runner. Editing a Draft does
not alter queued or retained material. Ordinary published deployments keep their
existing API route and catalogue transport. Unsupported servers cannot silently
substitute a published Default for a test.

The Environment destroy worker uses the authenticated runtime catalogue client
when retained graphs contain Draft snapshots. It reconstructs their exact source
without acquiring fresh authoring authority or re-reading the Draft head.

Storage migrations 36 and 37 retain exact interface and executable provenance.
Older binaries must not resume against the newer schema. Preserve retained
history; a destructive database downgrade is not a rollback mechanism.

Real isolated CP/IAM/DP/Runner, CLI, Terraform and OpenTofu execution is qualified.
Console, normal teardown and seeded cross-service upgrade/restart also pass.
The upgrade rehearsal uses exact source-built native binaries. Complete final
packaged upgrade, plugin compatibility, public SDK and release checks before
publishing this candidate.

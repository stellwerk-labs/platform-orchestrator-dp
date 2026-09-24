# Data Plane integration fixtures

Module Version Management requires a compatible CP, IAM, DP and Runner revision
set. CI requires `CORE_CP_INTEGRATION_IMAGE`, `CORE_IAM_INTEGRATION_IMAGE` and
`CORE_RUNNER_INTEGRATION_IMAGE` repository variables pinned by image digest.
Record their matching source revisions in the release manifest before
publication. CI fails early when a required candidate input is absent instead
of testing against a pre-versioning public image. The workflow needs no
credentials for private images.

Managed Module fixtures declare their known output interfaces explicitly. They
do not copy Resource Type schemas from the server or manufacture artifact
digests. External digest claims are optional; real legacy and managed deployment
tests still require the referenced artifact to be available on the Runner.

Local builds can use `CP_IMAGE`, `IAM_IMAGE`, and `RUNNER_IMAGE` Make overrides.
The Data Plane image is built from this worktree. The integration Makefile also
accepts `COMPOSE_PROJECT_NAME`, `NAME`, `PUBLIC_PORT`, `DATABASE_PORT`, `CP_PORT`,
`DP_PORT`, `IAM_PORT`, `NATS_PORT`, `NATS_HEALTH_PORT`, and `VAULT_PORT` so local
acceptance stacks do not collide with user-owned instances.

After generating `compose.yaml`, source `local-test-env.sh` **from this
directory**, with the same port variables, before running bounded test batches.
It exports the generated connection settings without displaying passwords.
Never use shell tracing or commit the generated Score state, kubeconfig,
database connection strings, Runner identity or Terraform state.

These fixtures use fixed, publicly known test authentication material and
disposable credentials. Published Compose ports may bind all host interfaces;
running locally does not by itself establish network isolation. Use only a
trusted, isolated test host. Never deploy these fixtures to shared, externally
exposed or production hosts, or reuse their credentials outside disposable
tests.

An optional `RUNNER_TEST_IMAGE` overrides only ordinary job fixtures. It can use
the test-only filesystem provider-mirror image documented in the Runner
repository. This runs the same candidate binary while avoiding external
registry download latency inside existing test deadlines; it is not a runtime
policy override.

`make test` also runs `make test-model-integration`. This uses the real local
PostgreSQL database in a uniquely named, temporary schema to apply the current
migrations and verify two simultaneous uses of a previously absent Deployment
idempotency key. The second transaction must wait, then return the original
Deployment with its exact Module Version request digest. The test removes only
its own schema. Run it separately against an existing generated stack with the
same `DATABASE_PORT` override; no CP or Runner rebuild is needed.

## Release-specific journeys

- `TestLegacyModuleExecutionAndHistoryRollback` exercises the exact migrated v0
  data shape through real CP, DP, Kubernetes and Runner execution. It verifies
  carry-forward, the first managed update, encrypted outputs and rollback to the
  captured original graph without manufacturing a historical digest. The CP
  populated-previous-schema migration test is a separate prerequisite; this
  runtime fixture alone does not prove an old-image installation upgrade.
- `TestManagedModulePinExecutionAndArchivedCarryForward` proves active Pin
  enforcement across a Default change, archived carry-forward of the older
  pinned version, note immutability, deliberate Unpin/adoption, archived active
  carry-forward without a Pin, and rejection of adoption into a new Environment.
- `TestManagedModuleLifecycleExecutionBoundaries` exercises Proposed/prerelease
  selection, restricted lifecycle adoption and Defective Pin protection with
  scoped IAM permissions and exact version confirmations.

The static-provider destroy fixture retains an older Deployment row briefly
using a database row lock while inspecting the real CP-triggered destroy. This
prevents fast successful Environment cleanup from deleting the evidence before
an API poll. The lock is released before asserting final Environment removal;
the production delete path and Runner are unchanged.

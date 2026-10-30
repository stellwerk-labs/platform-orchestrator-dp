# Draft test execution, current implementation

This branch is not a qualified release. The Control Plane captures immutable
Draft test material and checks explicit provenance at central admission. This
Data Plane branch now carries test selections through the ordinary deployment
engine. Complete CP/IAM/DP/Runner execution qualification remains outstanding.

The published catalogue's existing Go SDK types remain unchanged. Graph code
consumes the additive execution DTO, and the normal published transport is
adapted through the official generated models. A test entry must match its
snapshot's source, inputs, providers, dependencies, parameter types, Module UUID,
executable UUID and exact Resource Type contract. Published lifecycle metadata
is forbidden on a test entry. A retained graph cannot silently lose test
provenance or acquire it from an unknown published coordinate.

Graph configuration and Module artifact requirements retain the snapshot,
Module, Draft, revision, Project and Environment identities. Graph copies retain
those identities and contract metadata; provider substitution maps are still
independent per node. Deleted nodes retain historical metadata and provider
context without requiring removed inline modules to be fetched.

Migration 37 fences older binaries through the existing schema-version startup
check. It validates artifact/graph test references and binds them to the stored
Environment UUID. Immutable selections survive admission receipt reads and
recovery scans. Downgrade refuses retained test history in graphs, artifacts,
receipts or queued intents. An empty history can downgrade and restart safely.
Add-on plan snapshots reject Draft provenance.

## Request and execution path

POST `/orgs/{orgId}/module-test-deployments` accepts a normal `deployment` body
and a nonempty `draft_test_snapshots` map of stable Module UUIDs to captured
snapshot UUIDs. Fresh tests support deploy and plan-only. Rollback selects an
existing Deployment and reconstructs its retained test references. The separate
route prevents an older server from silently ignoring test fields and deploying
a published Default instead.

Queue intent and idempotency fingerprints retain exact snapshot selection,
not a moving authoring head. Promotion checks the current actor's authority.
Compilation requests the authenticated execution catalogue and validates every
returned test coordinate, Environment binding and frozen definition. Retained
test material receives no new rules. The standard graph and Terraform compiler
remain the execution engine.

Fresh admission derives the exact test subset from compiled artifact references,
validates Module, executable, Project and Environment identity, and persists the
subset in the durable receipt. The canonical admission digest adds the field
only when nonempty, preserving ordinary historical request bytes. Destroy keeps
its existing empty lifecycle-evidence exception.

Bundle downloads and worker dispatch use the configured Data Plane admission
identity through its configured authenticated admission gateway to read exact
retained inline sources. The ordinary internal CP client carries the system
identity and must not be used for this boundary. The gateway authenticates the
bearer token through IAM and supplies the trusted `From` identity; adding a
bearer header to the internal system client is insufficient. The runtime
configuration must route the exact `module-execution-catalogue` path to CP.
Bundle reconstruction requests only sources needed by live inline nodes; deleted
nodes retain provider context without becoming live executables. They verify test provenance
and contract metadata against persisted artifacts. A changed current Draft,
Default or Pin cannot substitute another source during reconstruction. This
material lookup grants no deployment authority; central admission remains the
mutation boundary.

Remaining qualification: execute real Runner tests; then prove restart,
rollback, permission revocation, Pins, clients, Console and upgrades. Saving or
planning a snapshot does not establish any execution outcome. Coordinated SDK
publication and standalone `GOWORK=off` qualification remain release gates.

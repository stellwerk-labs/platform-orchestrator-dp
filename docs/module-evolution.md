# Module evolution development

The Data Plane uses the matching Control Plane v3 SDK for ordinary clients
and execution graphs. The separate handwritten catalogue metadata structure has
been removed. Exact Resource Type interface UUIDs and version numbers travel
with graph nodes; the complete output declaration also travels with retained
artifact requirements, queued execution and Runner bundle manifests.

Inline bundle reconstruction compares its retained contract with the exact
catalogue result and refuses substitution. Historical records without contract
metadata remain readable. They do not acquire an invented interface identity.

Schema36 validates new contract metadata in retained Deployments and external
plans. The existing schema-version startup check stops older binaries that
would otherwise discard those JSON fields on reserialization. An explicit
schema downgrade refuses once any contract snapshot is retained. Qualify a
schema-compatible recovery image; do not roll back to an older DP binary after
recording the new execution format.

This is a coordinated development branch. The v3 SDKs are not yet published.
Before a standalone release, publish the qualified nested modules under
separate approval and verify their exact public dependency pins. Do not
publish an image that relies on local-only SDK resolution.

For local qualification, resolve both exact v3 SDK modules through the
reviewed private file proxy, then run `make generate`, `make test-unit`
and `make lint` with `GOWORK=off`. Final release qualification must
resolve the published modules and verify their public availability.

Draft authoring and immutable Proposed publication are implemented in the
matching Control Plane branch. Actual immutable Draft test execution is still
unfinished. This branch does not authorize a moving Draft as an executable
Module Version, bypass Environment Pins, or introduce another execution engine.

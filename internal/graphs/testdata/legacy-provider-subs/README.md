# Synthetic historical provider-substitution fixtures

These committed gob files were generated once by the independent standard-library
encoder in `encoder/`, with Go 1.26.10. Tests read the frozen files and never
regenerate them. All values are synthetic. No customer graph, SQL, state, logs,
identifiers, credentials or production data are included.

The `canyon_graph` package reconstructs the historical exported placeholder and
coordinate schema. Its own distinct concrete types produce the real historical
gob interface names (`*canyon_graph.ContextPlaceholder`, `TfVarPlaceholder`,
`ResourcePlaceholder`, `SelectorPlaceholder`). It imports neither today's graph
module nor DP types. The independent `orchestrator_graph` package supplies the
current-name replicas used only by mixed-name fixtures. The direct-map writer
uses the historical map shape; canonical-container fixtures exercise the same
legacy concrete names within the later container shape, without claiming that
this combination was emitted by an old release.

A verified pre-rename Git source/old release compiler was unavailable locally.
These are transparent wire-schema reconstructions, not artifacts attributed to
an unverified historical commit. The old concrete names were independently
reported by a retained-graph failure. Current field schemas were checked against
`platform-orchestrator-graph@v1.0.0`; the package has no pre-rename local history.
Gob matches nested concrete struct fields by name, so both pointer coordinates
and slices of coordinates decode structurally into current field types.

`historical-map`/`historical-canonical` contain all four variants and every field,
including nested coordinates, selector functions/arguments/outputs, resource
flags and empty provider substitutions. Mixed fixtures include an independently
encoded current-name placeholder alongside historical names. Negative fixtures
cover unknown concrete names and an unsupported canonical version.

The private local test receipt shows these exact frozen old/mixed fixtures fail
unmodified DP d110b66 and pass the compatibility decoder. That receipt is not a
claim of an executed existing-Environment upgrade. New writes retain the existing
canonical writer. No stored graph is eagerly rewritten.

To intentionally replace fixtures, run `GOWORK=off GOENV=off GOPROXY=off
GOTOOLCHAIN=local go run .` in `encoder/`, then review semantic tests and update
`SHA256SUMS`. Historical direct-map iteration is deliberately not canonical,
so regeneration need not reproduce identical bytes. Ordinary tests must use the
committed frozen bytes.

`historical-tfvar-integration.gob` contains only the synthetic `faulty` provider
substitution `${var.legacy_provider_flag}` → historical `TfVarPlaceholder` with
key `legacy_provider_flag`. It is generated separately with `go run . integration`
and is intended for a real existing-graph integration regression. This mode
leaves the other frozen files untouched.

# Platform Orchestrator Data Plane

The data plane is the Platform Orchestrator service responsible for deployments,
resource graphs, metadata values, and related per-environment APIs. During a
deployment it obtains module definitions and rules from the control plane and
manages the deployment lifecycle.

The source is maintained by [Stellwerk Labs](https://github.com/stellwerk-labs)
and the release image is published as
`ghcr.io/stellwerk-labs/platform-orchestrator-dp`.

## Development

Use the Go version declared in `go.mod`. Code generation also requires `yq`.

```shell
make install
make generate
make test-unit
go vet ./...
```

Run `make help` for the complete target list.

## Container

The production image builds entirely from public modules and does not require
Git credentials:

```shell
docker build --target final -t platform-orchestrator-dp:local .
```

## Integration Tests

Integration tests require Docker, Kind, kubectl, Helm, and Score Compose. Some
encrypted runner-log scenarios also require a service account with the admin
role for the configured log bucket.

```shell
make test-integration
make clean
```

If Kind cannot load an image from Docker Desktop, disabling Docker Desktop's
containerd image store may resolve the issue. See the
[Kind issue](https://github.com/kubernetes-sigs/kind/issues/3795) for context.

## Configuration and APIs

Runner timeout, unknown-outcome fencing and its audited acknowledgement are
documented in [Runner dispatch and completion recovery](./docs/runner-dispatch-recovery.md).

Runtime configuration is defined in
[`internal/config/config.go`](./internal/config/config.go). The OpenAPI contract
is in [`openapi/spec.yaml`](./openapi/spec.yaml); the service also exposes
`GET /alive` for liveness and `GET /health` for readiness.

Platform Orchestrator remains the product name. Existing service names, Score
resource names, API fields, and runtime identifiers therefore retain
`platform-orchestrator` names. They are product or compatibility surfaces, not
references to the former GitHub owner. Public source, dependency, release,
container, Go module, and request-local marker ownership uses `stellwerk-labs`.

## License

Licensed under the [European Union Public Licence 1.2](./LICENSE).

## Installation maintenance

`GET` and `PUT /platform-settings/deployments` expose persisted maintenance mode.
The typed contract is in `openapi/installation-settings.yaml`. An IAM operator
needs `platform_settings_read` or `platform_settings_write`, granted to explicit
human users through IAM's `PLATFORM_SETTINGS_READERS/WRITERS` configuration.
Writes use the quoted `If-Match` revision, exact Console origin for cookies, a
30-second request deadline and an audit record. The normal gateway supplies the
authenticated `From` identity; clients must not choose it themselves.

Enabling maintenance serializes with durable deployment/queue acceptance in the
same database. Existing deployments, exact idempotent retries and accepted queue
promotion continue. New ordinary, external, observation, plan and destroy
records are refused with `503 system_maintenance`. No internal actor bypasses the
gate. Reading, cancellation, Runner completion and settlement remain available.
Future Rollout stages and deferred destroy workflows without a committed Data
Plane record are new work; existing retries, TTLs and deadlines still apply.
The GUI counts executing and queued records, not a proof of stopped external
effects. Maintenance does not stop services or carry out upgrades.

All Data Plane replicas must be upgraded before relying on the gate. Its
singleton row is shared by those replicas and survives restarts. Use the matching
Helm routes and Console. The optional integration-tag
`TestInstallationSettingsPeer` is a local real-IAM browser qualification peer,
launched by IAM's cross-repository settings test rather than in normal CI.

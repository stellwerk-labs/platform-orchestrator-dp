# DP shared SDK v3 migration

The next DP generated Go SDK uses
`github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3`.
The intended first module tag is `shared/v3.0.0`; it has not been published.
The previously released `shared/v2` path and tags remain separate.

Update Go imports to `shared/v3/...` and pin `v3.0.0` only after the
approved nested-module tag and public Go proxy are verified. The parent
application uses its own `./shared` snapshot. CP and DP application builds
also require the other component's published v3 SDK.

Generated `genclient.ClientInterface` and
`genclient.ClientWithResponsesInterface` gain methods. Custom
implementations must add those methods. CP v3 also adds fields to existing
generated models; positional struct literals must be updated. Callers using
keyed literals and existing concrete client methods should review behavior
against the new API contract. This Go import-path migration does not qualify
server HTTP behavior, image packaging, or a client/server version pair.

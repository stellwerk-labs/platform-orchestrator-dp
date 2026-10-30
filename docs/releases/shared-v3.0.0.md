# DP shared SDK v3.0.0 (planned)

This nested Go module is planned for `shared/v3.0.0`, separately from the
application image release. Its source is the final Module Evolution export
plus the `shared/v3` import-path migration. The generated HTTP/OpenAPI
contract is unchanged by this migration.

The new major path reflects added methods on existing generated client
interfaces. CP also adds fields to existing exported generated models. See
[the migration guide](../sdk-v3-migration.md) for consumer changes.
Publication, public proxy availability, root application builds, and
product compatibility remain pending qualification and approval.

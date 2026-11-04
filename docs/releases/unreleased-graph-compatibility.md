# Retained graph compatibility (unreleased)

The data plane reads retained provider substitutions with historical Canyon-era
placeholder type names, including nested resource coordinates. This applies to
reference graphs used for new plans, deployment bundles, retained plans and
rollback. Both historical map and canonical-container shapes remain readable;
new graph writes keep the current canonical encoding.

No database history rewrite, Environment recreation, or operator conversion
command is required. Corrupt payloads, unsupported encoding versions and unknown
placeholder types remain errors. This fix does not change state identity, module
versions, migrations, or Runner ownership rules.

Local codec tests use independently encoded synthetic frozen fixtures. An actual
existing-Environment upgrade rehearsal remains a separate qualification gate;
this note does not claim publication or completed end-to-end upgrade validation.

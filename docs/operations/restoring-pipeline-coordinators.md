# Quarantine a restored pipeline coordinator

Set `RESTORE_QUARANTINE=true` before starting any restored data-plane process.
The process exits before opening the database, applying migrations, connecting
to other services or starting workers. This prevents restored pending admissions
from being declared absent solely because the restored snapshot lacks their
later Deployment records.

Quarantine intentionally provides no HTTP server or Console inspection.
Inspect the restored database with restricted PostgreSQL tools. A quarantined
pod never becomes ready, so a successful `helm --wait` is not the completion
criterion. Startup logs identify the hold without exposing retained commands.

1. Stop and fence every old coordinator and publisher before restoring data.
   Verify the fence across all replicas. Do not restore into a database still
   used by another running coordinator.
2. Retain Core admission history, DP receipts and backups, site journals,
   unknown-effect fences, state, artifacts and keys. Restore into an isolated
   destination with quarantine already configured.
3. Verify each replacement process receives `RESTORE_QUARANTINE=true` and
   refuses startup. Check the effective environment, including explicit `env`
   and Secret overrides. A ConfigMap value alone does not establish the hold.
4. Reconcile surviving admission and execution histories before allowing any
   coordinator to resume. Missing restored rows do not prove that execution
   never occurred. Preserve uncertain outcomes until actual stop evidence is
   available.
5. Restart with quarantine disabled only after that reconciliation and the
   old-coordinator fence have been checked. There is no HTTP action or automatic
   recovery rule that clears this startup setting.

An offline site may continue an already accepted command while central services
are stopped. Quarantine does not revoke it. Unreachable or possibly running
sites must remain unresolved before resuming central execution. See
[Runner dispatch recovery](../runner-dispatch-recovery.md).

The flag defaults to `false` and does not detect database rollback automatically.
It is a startup safety boundary, not a managed restore launcher or proof that a
particular cross-system restore is complete. Qualify the actual fenced restore
procedure, deployment configuration and Runner packaging before enabling the
production Native execution profile. Deliberately bypassing that procedure or
restoring Core history requires separate operator recovery.

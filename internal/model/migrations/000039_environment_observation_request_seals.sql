-- +goose Up
-- A retained negative receipt closes only its exact request. It never proves
-- anything about a previously admitted Runner or releases its ownership.
CREATE TABLE environment_observation_request_seals (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    request_kind TEXT NOT NULL CHECK (request_kind IN ('observation','redeploy')),
    request_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    run_id UUID,
    sealed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id,plugin_id,request_kind,request_id),
    CHECK ((request_kind='observation' AND run_id IS NULL) OR (request_kind='redeploy' AND run_id IS NOT NULL))
);
-- Already-running older replicas do not re-run the startup schema guard and
-- cannot inspect this new table. Their existing receipt insert is the last
-- transactional fence before publishing work, so protect that write as well.
-- +goose StatementBegin
CREATE FUNCTION environment_observation_seal_guard() RETURNS trigger AS $$
DECLARE
    sealed_kind TEXT := TG_ARGV[0];
    lock_prefix TEXT;
BEGIN
    -- An old transaction may have frozen its snapshot before the seal. Waiting
    -- on an advisory lock does not refresh REPEATABLE READ/SERIALIZABLE views.
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'observation request arbitration requires READ COMMITTED isolation'
            USING ERRCODE='25000', CONSTRAINT='environment_observation_read_committed_required';
    END IF;
    IF sealed_kind='observation' THEN
        lock_prefix := 'observation-request:';
    ELSIF sealed_kind='redeploy' THEN
        lock_prefix := 'observation-redeploy:';
    ELSE
        RAISE EXCEPTION 'invalid observation request kind';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(lock_prefix || NEW.org_id || ':' || NEW.plugin_id || ':' || NEW.request_id::text,0));
    IF EXISTS(SELECT 1 FROM environment_observation_request_seals seal
              WHERE seal.org_id=NEW.org_id AND seal.plugin_id=NEW.plugin_id
                AND seal.request_kind=sealed_kind AND seal.request_id=NEW.request_id) THEN
        RAISE EXCEPTION 'observation request was sealed without starting'
            USING ERRCODE='23514', CONSTRAINT='environment_observation_request_not_started';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE TRIGGER environment_observation_receipts_seal_guard BEFORE INSERT ON environment_observation_receipts
FOR EACH ROW EXECUTE FUNCTION environment_observation_seal_guard('observation');
CREATE TRIGGER environment_observation_redeploy_receipts_seal_guard BEFORE INSERT ON environment_observation_redeploy_receipts
FOR EACH ROW EXECUTE FUNCTION environment_observation_seal_guard('redeploy');
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM environment_observation_request_seals) THEN
    RAISE EXCEPTION 'cannot discard sealed observation requests; use compatible forward recovery';
  END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER environment_observation_receipts_seal_guard ON environment_observation_receipts;
DROP TRIGGER environment_observation_redeploy_receipts_seal_guard ON environment_observation_redeploy_receipts;
DROP FUNCTION environment_observation_seal_guard();
DROP TABLE environment_observation_request_seals;

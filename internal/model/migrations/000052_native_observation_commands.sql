-- +goose Up
ALTER TABLE deployment_dispatch_budgets ADD COLUMN observation_command BYTEA;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM deployment_dispatch_budgets WHERE observation_command IS NOT NULL) THEN
  RAISE EXCEPTION 'cannot discard native observation command evidence';
 END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE deployment_dispatch_budgets DROP COLUMN observation_command;

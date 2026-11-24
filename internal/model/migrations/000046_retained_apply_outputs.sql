-- +goose Up
ALTER TABLE retained_plans ADD COLUMN apply_outputs BYTEA;

-- +goose Down
ALTER TABLE retained_plans DROP COLUMN apply_outputs;

package model

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
)

func (d *databaser) CreateMetadataKey(ctx context.Context, optionalTx Tx, orgId string, key *MetadataKey) (*MetadataKey, error) {
	now := time.Now().UTC()
	key.CreatedAt = now

	_, err := d.txOrDb(optionalTx).ExecContext(
		ctx,
		`INSERT INTO metadata_keys (org_id, name, description, type, format, pattern, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		orgId, key.Name, key.Description, key.Schema.Type, key.Schema.Format, key.Schema.Pattern, key.CreatedAt,
	)
	if err != nil {
		if pqe := new(pq.Error); errors.As(err, &pqe) {
			if pqe.Constraint == "metadata_keys_pkey" {
				return nil, NewErrConflict(fmt.Sprintf("metadata_keys with name %s already exists", key.Name))
			}
		}
		return nil, errors.Wrap(err, "failed to insert metadata key")
	}
	return key, nil
}

func (d *databaser) GetMetadataKey(ctx context.Context, optionalTx Tx, orgId string, keyName string) (*MetadataKey, error) {
	row := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		`SELECT name, description, type, format, pattern, created_at FROM metadata_keys WHERE org_id = $1 AND name = $2`,
		orgId, keyName,
	)
	var key MetadataKey
	err := row.Scan(&key.Name, &key.Description, &key.Schema.Type, &key.Schema.Format, &key.Schema.Pattern, &key.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NewErrNotFound("metadata key not found")
		}
		return nil, errors.Wrap(err, "failed to scan metadata key")
	}
	return &key, nil
}

func (d *databaser) ListMetadataKeys(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int) ([]*MetadataKey, string, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	limitPlusOne := max(1, perPage) + 1

	rows, err := d.txOrDb(optionalTx).QueryContext(
		ctx,
		`SELECT name, description, type, format, pattern, created_at FROM metadata_keys WHERE org_id = $1 AND name > $2 ORDER BY name LIMIT $3`,
		orgId, pageToken, limitPlusOne,
	)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to query metadata keys")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logger.Error("failed to close row set")
		}
	}()

	out := make([]*MetadataKey, 0, limitPlusOne-1)
	for rows.Next() {
		var key MetadataKey
		err := rows.Scan(&key.Name, &key.Description, &key.Schema.Type, &key.Schema.Format, &key.Schema.Pattern, &key.CreatedAt)
		if err != nil {
			return nil, "", errors.Wrap(err, "failed to scan metadata key")
		}

		if len(out) >= limitPlusOne-1 {
			last := out[len(out)-1]
			return out, last.Name, nil
		}
		out = append(out, &key)
	}
	if rows.Err() != nil {
		return nil, "", errors.Wrap(rows.Err(), "failed to iterate rows")
	}
	return out, "", nil
}

func (d *databaser) UpdateMetadataKey(ctx context.Context, optionalTx Tx, orgId string, key *MetadataKey) error {
	result, err := d.txOrDb(optionalTx).ExecContext(
		ctx,
		`UPDATE metadata_keys SET description = $1, type = $2, format = $3, pattern = $4 WHERE org_id = $5 AND name = $6`,
		key.Description, key.Schema.Type, key.Schema.Format, key.Schema.Pattern, orgId, key.Name,
	)
	if err != nil {
		return errors.Wrap(err, "failed to update metadata key")
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}
	if rowsAffected == 0 {
		return NewErrNotFound("metadata key not found")
	}
	return nil
}

func (d *databaser) DeleteMetadataKey(ctx context.Context, optionalTx Tx, orgId string, keyName string) error {
	_, err := d.txOrDb(optionalTx).ExecContext(
		ctx,
		`DELETE FROM metadata_keys WHERE org_id = $1 AND name = $2`,
		orgId, keyName,
	)
	if err != nil {
		return errors.Wrap(err, "failed to delete metadata key")
	}
	return nil
}

package model

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
)

// Accepted references are local authority, never replaceable by Runner progress.
func AddPipelineLogReference(ctx context.Context, tx Tx, id uuid.UUID, descriptor nativelogs.Descriptor) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT progress FROM deployment_pipeline_runs WHERE deployment_id=$1 FOR UPDATE`, id).Scan(&raw); err != nil {
		return err
	}
	var progress PipelineProgress
	if err := json.Unmarshal(raw, &progress); err != nil {
		return err
	}
	references, limited, err := appendPipelineLogReference(progress.LogReferences, progress.LogReferencesLimited, descriptor)
	if err != nil {
		return err
	}
	referencesJSON, err := json.Marshal(references)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET progress=jsonb_set(jsonb_set(progress,'{log_references}',$2::jsonb),'{log_references_limited}',to_jsonb($3::boolean)),updated_at=now() WHERE deployment_id=$1`, id, referencesJSON, limited)
	return err
}

func appendPipelineLogReference(references []nativelogs.Descriptor, limited bool, descriptor nativelogs.Descriptor) ([]nativelogs.Descriptor, bool, error) {
	if descriptor.Validate() != nil || descriptor.Status != nativelogs.PendingTransfer {
		return nil, limited, errors.New("invalid native log reference")
	}
	for _, existing := range references {
		a, b := existing.Authority, descriptor.Authority
		if a.Phase == b.Phase && a.ExecutionID == b.ExecutionID && a.CaptureRevision == b.CaptureRevision {
			if existing != descriptor {
				return nil, limited, NewErrConflict("native log capture identity conflict")
			}
			return references, limited, nil
		}
	}
	candidate := append(append([]nativelogs.Descriptor{}, references...), descriptor)
	raw, err := json.Marshal(candidate)
	if err != nil {
		return nil, limited, err
	}
	if len(candidate) > nativelogs.ReferenceLimit || len(raw) > nativelogs.ReferenceBytesLimit {
		return references, true, nil
	}
	return candidate, limited, nil
}

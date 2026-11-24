package model

import (
	"context"
	"fmt"
)

// A dangling state head must not disappear through the ordinary listing's
// inner join and masquerade as proof of unused provider material.
func CheckProviderStateCoverage(ctx context.Context, tx Tx, org string) error {
	var incomplete bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM deployment_environments e
		JOIN deployment_effective_state effective ON effective.environment_id=e.id
		LEFT JOIN deployments candidate ON candidate.id=e.last_state_deployment_id AND candidate.de_id=e.id
		LEFT JOIN deployments d ON d.id=effective.deployment_id AND d.de_id=e.id
		WHERE e.org_id=$1 AND (candidate.id IS NULL OR (effective.deployment_id IS NOT NULL AND d.id IS NULL)))`, org).Scan(&incomplete)
	if err != nil {
		return err
	}
	if incomplete {
		return fmt.Errorf("managed state head is unavailable")
	}
	return nil
}

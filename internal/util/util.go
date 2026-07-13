package util

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

func GenerateHashedRunnerToken(secret, orgID, deploymentID string) string {
	h := sha256.New()
	_, _ = fmt.Fprint(h, secret, orgID, deploymentID)
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func GenerateNodeHash(depEnvUuid uuid.UUID, resourceType string, resourceClass string, resourceId string) string {
	h := sha256.New()
	_, _ = fmt.Fprint(h, depEnvUuid, " ", resourceType, " ", resourceClass, " ", resourceId)
	return hex.EncodeToString(h.Sum(nil))
}

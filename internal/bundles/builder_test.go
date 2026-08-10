package bundles

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestObjectContract(t *testing.T) {
	deploymentID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	assert.Equal(t, "org-1/"+deploymentID.String(), ObjectKey("org-1", deploymentID))
	assert.Greater(t, ObjectStoreTTL, 30*24*time.Hour)
}

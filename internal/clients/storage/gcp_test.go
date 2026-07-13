package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewGCPStorageClient_WithInvalidCredentials_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	// Invalid JSON credentials should fail
	_, err := NewGCPStorageClient(ctx, "invalid-json-credentials", logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GCS client with specified credentials")
}

func TestNewGCPStorageClient_WithMalformedJSONCredentials_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	// Malformed JSON that's valid JSON but not valid GCP credentials
	_, err := NewGCPStorageClient(ctx, `{"type": "invalid_type", "project_id": "test"}`, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GCS client with specified credentials")
}

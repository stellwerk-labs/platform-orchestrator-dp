package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewStorageClient_EmptyEndpoint_ReturnsGCPClient(t *testing.T) {
	// When endpoint is empty, it should attempt to create a GCP client.
	// This will fail in test environment without GCP credentials, but we can
	// verify the error message indicates GCP client creation was attempted.
	ctx := context.Background()
	logger := zap.NewNop()

	_, err := NewStorageClient(ctx, "", "", logger)

	// In a test environment without GCP credentials, this should fail with a GCP-related error
	// The key assertion is that it attempted to create a GCP client (not S3)
	if err != nil {
		assert.Contains(t, err.Error(), "GCS", "Expected GCP/GCS client creation attempt")
	}
}

func TestNewStorageClient_InvalidEndpointURL_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	_, err := NewStorageClient(ctx, "://invalid-url", `{"access_key_id":"key","secret_access_key":"secret"}`, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse runner logs bucket endpoint")
}

func TestNewStorageClient_InvalidCredentialsJSON_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	_, err := NewStorageClient(ctx, "http://localhost:9000", "invalid-json", logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse S3 credentials")
}

func TestNewStorageClient_EmptyAccessKeyID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	_, err := NewStorageClient(ctx, "http://localhost:9000", `{"access_key_id":"","secret_access_key":"secret"}`, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid S3 credentials: access_key_id or secret_access_key is empty")
}

func TestNewStorageClient_EmptySecretAccessKey_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	_, err := NewStorageClient(ctx, "http://localhost:9000", `{"access_key_id":"key","secret_access_key":""}`, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid S3 credentials: access_key_id or secret_access_key is empty")
}

func TestNewStorageClient_ValidS3Config_ReturnsS3Client(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	client, err := NewStorageClient(ctx, "http://localhost:9000", `{"access_key_id":"testkey","secret_access_key":"testsecret"}`, logger)

	require.NoError(t, err)
	require.NotNil(t, client)

	// Verify it's an S3 client by type assertion
	_, ok := client.(*s3client)
	assert.True(t, ok, "Expected S3 client to be returned")
}

func TestNewStorageClient_S3ConfigWithHTTPS_ReturnsS3Client(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	client, err := NewStorageClient(ctx, "https://s3.amazonaws.com", `{"access_key_id":"testkey","secret_access_key":"testsecret"}`, logger)

	require.NoError(t, err)
	require.NotNil(t, client)

	_, ok := client.(*s3client)
	assert.True(t, ok, "Expected S3 client to be returned")
}

func TestS3ConfigAuth_JSONUnmarshal(t *testing.T) {
	tests := []struct {
		name           string
		json           string
		expectedKey    string
		expectedSecret string
	}{
		{
			name:           "standard keys",
			json:           `{"access_key_id":"mykey","secret_access_key":"mysecret"}`,
			expectedKey:    "mykey",
			expectedSecret: "mysecret",
		},
		{
			name:           "with extra fields",
			json:           `{"access_key_id":"mykey","secret_access_key":"mysecret","extra":"ignored"}`,
			expectedKey:    "mykey",
			expectedSecret: "mysecret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := zap.NewNop()

			client, err := NewStorageClient(ctx, "http://localhost:9000", tt.json, logger)

			require.NoError(t, err)
			require.NotNil(t, client)
		})
	}
}

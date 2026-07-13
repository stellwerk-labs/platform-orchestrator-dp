package storage

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewS3StorageClient_ValidConfig_ReturnsClient(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	endpoint, _ := url.Parse("http://localhost:9000")
	cfg := S3Config{
		Endpoint: *endpoint,
		Auth: S3ConfigAuth{
			AccessKeyID:     "testkey",
			SecretAccessKey: "testsecret",
		},
	}

	client, err := NewS3StorageClient(ctx, cfg, logger)

	require.NoError(t, err)
	require.NotNil(t, client)

	s3c, ok := client.(*s3client)
	assert.True(t, ok, "Expected s3client type")
	assert.NotNil(t, s3c.client)
	assert.NotNil(t, s3c.presignClient)
	assert.Equal(t, logger, s3c.logger)
}

func TestNewS3StorageClient_WithHTTPSEndpoint_ReturnsClient(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	endpoint, _ := url.Parse("https://s3.amazonaws.com")
	cfg := S3Config{
		Endpoint: *endpoint,
		Auth: S3ConfigAuth{ //nolint:gosec
			AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
	}

	client, err := NewS3StorageClient(ctx, cfg, logger)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNewS3StorageClient_WithMinioEndpoint_ReturnsClient(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	endpoint, _ := url.Parse("http://minio.local:9000")
	cfg := S3Config{
		Endpoint: *endpoint,
		Auth: S3ConfigAuth{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	}

	client, err := NewS3StorageClient(ctx, cfg, logger)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNewS3StorageClient_WithEmptyEndpoint_ReturnsClient(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	// Empty URL is still a valid url.URL
	cfg := S3Config{
		Endpoint: url.URL{},
		Auth: S3ConfigAuth{
			AccessKeyID:     "testkey",
			SecretAccessKey: "testsecret",
		},
	}

	client, err := NewS3StorageClient(ctx, cfg, logger)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestS3Client_ImplementsStorageInterface(t *testing.T) {
	// Compile-time check that s3client implements Storage interface
	var _ Storage = (*s3client)(nil)
}

func TestS3Config_StructFields(t *testing.T) {
	endpoint, _ := url.Parse("http://localhost:9000")
	cfg := S3Config{
		Endpoint: *endpoint,
		Auth: S3ConfigAuth{
			AccessKeyID:     "mykey",
			SecretAccessKey: "mysecret",
		},
	}

	assert.Equal(t, "localhost:9000", cfg.Endpoint.Host)
	assert.Equal(t, "http", cfg.Endpoint.Scheme)
	assert.Equal(t, "mykey", cfg.Auth.AccessKeyID)
	assert.Equal(t, "mysecret", cfg.Auth.SecretAccessKey)
}

func TestS3ConfigAuth_StructFields(t *testing.T) {
	auth := S3ConfigAuth{ //nolint:gosec
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", auth.AccessKeyID)
	assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", auth.SecretAccessKey)
}

func TestDefaultS3Region(t *testing.T) {
	assert.Equal(t, "us-east-1", defaultS3Region)
}

func TestNewS3StorageClient_CreatesPresignClient(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	endpoint, _ := url.Parse("http://localhost:9000")
	cfg := S3Config{
		Endpoint: *endpoint,
		Auth: S3ConfigAuth{
			AccessKeyID:     "testkey",
			SecretAccessKey: "testsecret",
		},
	}

	client, err := NewS3StorageClient(ctx, cfg, logger)

	require.NoError(t, err)

	s3c := client.(*s3client)
	assert.NotNil(t, s3c.presignClient, "Presign client should be initialized")
}

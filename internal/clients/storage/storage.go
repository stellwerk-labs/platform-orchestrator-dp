package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ObjectIterator provides an interface for iterating over storage objects.
// Call Next() repeatedly until it returns io.EOF.
type ObjectIterator interface {
	// Next returns the next object name. Returns io.EOF when there are no more objects.
	// Returns other errors if the iteration fails.
	Next() (string, error)
}

type Storage interface {
	GetReader(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, bucketName, objectName string) error
	GetPresignedURL(ctx context.Context, bucketName, objectName string, expiresAfter time.Duration) (string, error)
	ListObjects(ctx context.Context, bucketName, prefix string) ObjectIterator
}

func NewStorageClient(ctx context.Context, runnerLogsBucketEndpoint, creds string, logger *zap.Logger) (Storage, error) {
	if runnerLogsBucketEndpoint == "" {
		return NewGCPStorageClient(ctx, creds, logger)
	}

	parsed, err := url.Parse(runnerLogsBucketEndpoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse runner logs bucket endpoint")
	}

	var auth S3ConfigAuth
	if err := json.Unmarshal([]byte(creds), &auth); err != nil {
		return nil, errors.Wrap(err, "failed to parse S3 credentials")
	} else if auth.AccessKeyID == "" || auth.SecretAccessKey == "" {
		return nil, errors.New("invalid S3 credentials: access_key_id or secret_access_key is empty")
	}

	return NewS3StorageClient(ctx, S3Config{
		Endpoint: *parsed,
		Auth:     auth,
	}, logger)
}

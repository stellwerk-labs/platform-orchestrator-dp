package storage

import (
	"context"
	"io"
	"net/http"
	"time"

	gcpstorage "cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type gcsclient struct {
	storageClient *gcpstorage.Client
	logger        *zap.Logger
}

// gcsObjectIterator wraps the GCS object iterator to implement ObjectIterator interface.
type gcsObjectIterator struct {
	it *gcpstorage.ObjectIterator
}

func (i *gcsObjectIterator) Next() (string, error) {
	attr, err := i.it.Next()
	if err == iterator.Done {
		return "", io.EOF
	}
	if err != nil {
		return "", err
	}
	return attr.Name, nil
}

func NewGCPStorageClient(ctx context.Context, creds string, logger *zap.Logger) (Storage, error) {
	if creds == "" {
		if storageClient, err := gcpstorage.NewClient(ctx); err != nil {
			return nil, errors.Wrap(err, "Failed to create GCS client with workload identity")
		} else {
			return &gcsclient{storageClient: storageClient, logger: logger}, nil
		}
	} else {
		if storageClient, err := gcpstorage.NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(creds))); err != nil {
			return nil, errors.Wrap(err, "Failed to create GCS client with specified credentials")
		} else {
			zap.L().Info("Using specified credentials for runner logs bucket")
			return &gcsclient{storageClient: storageClient}, nil
		}
	}
}

func (c *gcsclient) GetReader(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error) {
	if reader, err := c.storageClient.Bucket(bucketName).Object(objectName).NewReader(ctx); err != nil {
		return nil, errors.Wrap(err, "Failed to create GCS object reader")
	} else {
		return reader, nil
	}
}

func (c *gcsclient) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	bucket := c.storageClient.Bucket(bucketName)
	if err := bucket.Object(objectName).Delete(ctx); err != nil {
		return errors.Wrapf(err, "failed to delete object %s", objectName)
	}
	return nil
}

func (c *gcsclient) GetPresignedURL(ctx context.Context, bucketName, objectName string, expiresAfter time.Duration) (string, error) {
	return c.storageClient.Bucket(bucketName).SignedURL(objectName, &gcpstorage.SignedURLOptions{
		Scheme:  gcpstorage.SigningSchemeV4,
		Method:  http.MethodPut,
		Expires: time.Now().Add(expiresAfter),
	})
}

func (c *gcsclient) ListObjects(ctx context.Context, bucketName, prefix string) ObjectIterator {
	it := c.storageClient.Bucket(bucketName).Objects(ctx, &gcpstorage.Query{Prefix: prefix})
	return &gcsObjectIterator{it: it}
}

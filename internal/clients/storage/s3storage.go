package storage

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const defaultS3Region = "us-east-1"

type s3client struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	logger        *zap.Logger
}

// s3ObjectIterator wraps the S3 paginator to implement ObjectIterator interface.
type s3ObjectIterator struct {
	paginator *s3.ListObjectsV2Paginator
	ctx       context.Context
	current   []types.Object
	index     int
}

func (i *s3ObjectIterator) Next() (string, error) {
	// If we've exhausted the current page, fetch the next one
	if i.index >= len(i.current) {
		if !i.paginator.HasMorePages() {
			return "", io.EOF
		}

		page, err := i.paginator.NextPage(i.ctx)
		if err != nil {
			return "", errors.Wrap(err, "failed to fetch next page of S3 objects")
		}

		i.current = page.Contents
		i.index = 0

		// If the page is empty, try to get the next page
		if len(i.current) == 0 {
			return i.Next()
		}
	}

	// Return the current object and advance the index
	obj := i.current[i.index]
	i.index++

	if obj.Key == nil {
		// Skip objects with nil keys
		return i.Next()
	}

	return *obj.Key, nil
}

type S3Config struct {
	Endpoint url.URL
	Auth     S3ConfigAuth
}

type S3ConfigAuth struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func NewS3StorageClient(ctx context.Context, cfg S3Config, logger *zap.Logger) (Storage, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(defaultS3Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.Auth.AccessKeyID, cfg.Auth.SecretAccessKey, ""),
		),
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load AWS config")
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint.String())
		o.UsePathStyle = true
	})
	presignClient := s3.NewPresignClient(client)

	return &s3client{
		client:        client,
		presignClient: presignClient,
		logger:        logger,
	}, nil
}

func (c *s3client) GetReader(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error) {
	output, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get S3 object")
	}
	return output.Body, nil
}

func (c *s3client) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to delete S3 object %s", objectName)
	}
	return nil
}

func (c *s3client) GetPresignedURL(ctx context.Context, bucketName, objectName string, expiresAfter time.Duration) (string, error) {
	presignedReq, err := c.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}, s3.WithPresignExpires(expiresAfter))
	if err != nil {
		return "", errors.Wrap(err, "failed to generate presigned URL for S3 object")
	}
	return presignedReq.URL, nil
}

func (c *s3client) ListObjects(ctx context.Context, bucketName, prefix string) ObjectIterator {
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	return &s3ObjectIterator{
		paginator: paginator,
		ctx:       ctx,
		current:   nil,
		index:     0,
	}
}

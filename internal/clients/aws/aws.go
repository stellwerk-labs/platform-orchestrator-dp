package aws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"golang.org/x/oauth2"
)

type AWSCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
	Region          string `json:"region"`
	Expiration      string `json:"expiration"`
}

type AccessTokenInfo struct {
	Principal string
}

type ClusterInfo struct {
	Name                     string
	Region                   string
	Endpoint                 string
	CertificateAuthorityData []byte
}

//go:generate go tool mockgen -destination mocks/aws_mock.go -package=mocks github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws CredentialsClientInterface

type CredentialsClientInterface interface {
	GetAssumeRoleTokenSource(ctx context.Context, roleArn, sessionName, idToken, stsRegion string) (oauth2.TokenSource, error)
	GetAccessTokenInfo(ctx context.Context, accessToken string) (*AccessTokenInfo, error)
	GetClusterInfo(ctx context.Context, clusterName, clusterRegion string, awsCredentials AWSCredentials) (*ClusterInfo, error)
	GenerateEksToken(ctx context.Context, clusterName, stsRegion string, awsCredentials AWSCredentials) (string, error)
}

type CredentialsClient struct{}

func NewCredentialsClient() CredentialsClientInterface {
	return &CredentialsClient{}
}

func (c *CredentialsClient) GetAssumeRoleTokenSource(ctx context.Context, roleArn, sessionName, idToken, stsRegion string) (oauth2.TokenSource, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS config: %w", err)
	}
	cfg.Region = stsRegion
	stsClient := sts.NewFromConfig(cfg)

	return &awsAssumeRoleTokenSource{
		stsClient:   stsClient,
		roleArn:     roleArn,
		sessionName: sessionName,
		idToken:     idToken,
		region:      stsRegion,
	}, nil
}

func (c *CredentialsClient) GetAccessTokenInfo(ctx context.Context, accessToken string) (*AccessTokenInfo, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS config: %w", err)
	}
	stsClient := sts.NewFromConfig(cfg)

	result, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to get caller identity: %w", err)
	}

	return &AccessTokenInfo{
		Principal: *result.Arn,
	}, nil
}

func (c *CredentialsClient) GetClusterInfo(ctx context.Context, clusterName, clusterRegion string, awsCredentials AWSCredentials) (*ClusterInfo, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(clusterRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsCredentials.AccessKeyID,
			awsCredentials.SecretAccessKey,
			awsCredentials.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	eksClient := eks.NewFromConfig(cfg)

	clusterInfo, err := eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{
		Name: aws.String(clusterName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe EKS cluster: %w", err)
	}

	if clusterInfo.Cluster == nil {
		return nil, fmt.Errorf("cluster info is nil")
	}

	if clusterInfo.Cluster.CertificateAuthority == nil {
		return nil, fmt.Errorf("certificate authority data is nil")
	}

	if clusterInfo.Cluster.Endpoint == nil {
		return nil, fmt.Errorf("cluster endpoint is nil")
	}

	caData, err := base64.StdEncoding.DecodeString(*clusterInfo.Cluster.CertificateAuthority.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode certificate authority data: %w", err)
	}

	return &ClusterInfo{
		Name:                     clusterName,
		Region:                   clusterRegion,
		Endpoint:                 *clusterInfo.Cluster.Endpoint,
		CertificateAuthorityData: caData,
	}, nil
}

func (c *CredentialsClient) GenerateEksToken(ctx context.Context, clusterName, stsRegion string, awsCredentials AWSCredentials) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(stsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsCredentials.AccessKeyID,
			awsCredentials.SecretAccessKey,
			awsCredentials.SessionToken,
		)),
	)
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}

	stsClient := sts.NewFromConfig(cfg)

	preSignClient := sts.NewPresignClient(stsClient)
	presignedURL, err := preSignClient.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}, func(opt *sts.PresignOptions) {
		opt.Presigner = newCustomHTTPPresignerV4(opt.Presigner, map[string]string{
			"x-k8s-aws-id":  clusterName,
			"X-Amz-Expires": "60",
		})
	})

	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL for EKS token: %w", err)
	}

	token := fmt.Sprintf("k8s-aws-v1.%s", base64.RawStdEncoding.EncodeToString([]byte(presignedURL.URL)))
	return token, nil
}

type awsAssumeRoleTokenSource struct {
	stsClient   *sts.Client
	roleArn     string
	sessionName string
	idToken     string
	region      string
}

func (a *awsAssumeRoleTokenSource) Token() (*oauth2.Token, error) {
	input := &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(a.roleArn),
		RoleSessionName:  aws.String(a.sessionName),
		WebIdentityToken: aws.String(a.idToken),
		DurationSeconds:  aws.Int32(int32(time.Hour.Seconds())),
	}

	result, err := a.stsClient.AssumeRoleWithWebIdentity(context.Background(), input)
	if err != nil {
		if IsAssumeRoleError(err) {
			return nil, usererrors.NewUserError(fmt.Sprintf("failed to assume role with web identity: %s", err.Error()))
		}
		return nil, fmt.Errorf("failed to assume role with web identity: %w", err)
	}

	credData := AWSCredentials{
		AccessKeyID:     *result.Credentials.AccessKeyId,
		SecretAccessKey: *result.Credentials.SecretAccessKey,
		SessionToken:    *result.Credentials.SessionToken,
		Region:          a.region,
		Expiration:      result.Credentials.Expiration.Format(time.RFC3339),
	}

	tokenData, err := json.Marshal(credData) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("failed to marshal credentials data: %w", err)
	}

	return &oauth2.Token{
		AccessToken: string(tokenData),
		Expiry:      *result.Credentials.Expiration,
	}, nil
}

type customHTTPPresignerV4 struct {
	client  sts.HTTPPresignerV4
	headers map[string]string
}

func newCustomHTTPPresignerV4(client sts.HTTPPresignerV4, headers map[string]string) sts.HTTPPresignerV4 {
	return &customHTTPPresignerV4{
		client:  client,
		headers: headers,
	}
}

func (p *customHTTPPresignerV4) PresignHTTP(
	ctx context.Context, credentials aws.Credentials, r *http.Request,
	payloadHash string, service string, region string, signingTime time.Time,
	optFns ...func(*v4.SignerOptions),
) (url string, signedHeader http.Header, err error) {
	for key, val := range p.headers {
		r.Header.Add(key, val)
	}
	return p.client.PresignHTTP(ctx, credentials, r, payloadHash, service, region, signingTime, optFns...)
}

func isGenericApiErrorWithCodes(err error, codes ...string) (*smithy.OperationError, bool) {
	var oe *smithy.OperationError
	if errors.As(err, &oe) {
		errMsg := oe.Err.Error()
		for _, code := range codes {
			if strings.Contains(errMsg, code) {
				return oe, true
			}
		}
	}
	return nil, false
}

func IsAccessDeniedException(err error) (*smithy.OperationError, bool) {
	return isGenericApiErrorWithCodes(err, "AccessDeniedException")
}

func IsAccessDenied(err error) (*smithy.OperationError, bool) {
	return isGenericApiErrorWithCodes(err, "AccessDenied")
}

// IsAssumeRoleError checks if an error is related to STS AssumeRole operations.
// These errors indicate user configuration issues (missing OIDC provider, invalid IAM roles,
// permission issues, expired tokens, etc.) and should be classified as UserError.
func IsAssumeRoleError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common STS AssumeRole error codes
	stsErrorCodes := []string{
		"InvalidIdentityToken",    // OIDC provider not found or invalid token
		"AccessDenied",            // IAM permission issues
		"AccessDeniedException",   // IAM permission issues (alternative form)
		"ExpiredToken",            // Token expiration
		"ExpiredTokenException",   // Token expiration (alternative form)
		"IDPRejectedClaim",        // OIDC claim validation failures
		"InvalidParameterValue",   // Invalid role ARN or configuration
		"MalformedPolicyDocument", // Invalid assume role policy
		"PackedPolicyTooLarge",    // Policy size issues
		"RegionDisabledException", // Region not enabled
	}

	if _, ok := isGenericApiErrorWithCodes(err, stsErrorCodes...); ok {
		return true
	}

	// Check for HTTP 4xx status codes which indicate client/user errors
	if strings.Contains(err.Error(), "StatusCode: 4") {
		return true
	}

	return false
}

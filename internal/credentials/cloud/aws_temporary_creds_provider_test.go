package cloud

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/smithy-go"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/oauth2"

	awsclient "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	aws_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws/mocks"
	oidc_mocks "github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc/mocks"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
)

func TestAwsTemporaryCredsProvider_InvalidIdentityToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stsRegion := testStsRegion

	// Simulate InvalidIdentityToken error from STS
	stsError := &smithy.OperationError{
		Err: errors.New("InvalidIdentityToken: No OpenIDConnect provider found in your account for https://oidc.platform-orchestrator.dev"),
	}

	a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
	a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
		Return(&awsErrorTokenSource{Error: stsError}, nil)

	o := oidc_mocks.NewMockProvider(ctrl)
	o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
		Return("some-id-token", nil)

	provider := &AwsTemporaryCredsProvider{
		OidcProvider:      o,
		CredentialsClient: a,
	}

	_, err := provider.ExchangeOauthToken(context.Background(), "some-org", "some-runner", testStsRegion, platformorchestratorcp.AwsTemporaryAuth{
		RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
		StsRegion: &stsRegion,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token")

	// Verify the error is classified as UserError
	var ue *usererrors.UserError
	assert.ErrorAs(t, err, &ue, "error should be classified as UserError")
}

func TestAwsTemporaryCredsProvider_AccessDenied(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stsRegion := testStsRegion

	// Simulate AccessDenied error from STS
	stsError := &smithy.OperationError{
		Err: errors.New("AccessDenied: User is not authorized to perform: sts:AssumeRoleWithWebIdentity"),
	}

	a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
	a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
		Return(&awsErrorTokenSource{Error: stsError}, nil)

	o := oidc_mocks.NewMockProvider(ctrl)
	o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
		Return("some-id-token", nil)

	provider := &AwsTemporaryCredsProvider{
		OidcProvider:      o,
		CredentialsClient: a,
	}

	_, err := provider.ExchangeOauthToken(context.Background(), "some-org", "some-runner", testStsRegion, platformorchestratorcp.AwsTemporaryAuth{
		RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
		StsRegion: &stsRegion,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token")

	// Verify the error is classified as UserError
	var ue *usererrors.UserError
	assert.ErrorAs(t, err, &ue, "error should be classified as UserError")
}

func TestAwsTemporaryCredsProvider_ExpiredToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stsRegion := testStsRegion

	// Simulate ExpiredToken error from STS
	stsError := &smithy.OperationError{
		Err: errors.New("ExpiredToken: The security token included in the request is expired"),
	}

	a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
	a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
		Return(&awsErrorTokenSource{Error: stsError}, nil)

	o := oidc_mocks.NewMockProvider(ctrl)
	o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
		Return("some-id-token", nil)

	provider := &AwsTemporaryCredsProvider{
		OidcProvider:      o,
		CredentialsClient: a,
	}

	_, err := provider.ExchangeOauthToken(context.Background(), "some-org", "some-runner", testStsRegion, platformorchestratorcp.AwsTemporaryAuth{
		RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
		StsRegion: &stsRegion,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token")

	// Verify the error is classified as UserError
	var ue *usererrors.UserError
	assert.ErrorAs(t, err, &ue, "error should be classified as UserError")
}

func TestAwsTemporaryCredsProvider_IDPRejectedClaim(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stsRegion := testStsRegion

	// Simulate IDPRejectedClaim error from STS
	stsError := &smithy.OperationError{
		Err: errors.New("IDPRejectedClaim: The identity provider rejected the claim"),
	}

	a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
	a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
		Return(&awsErrorTokenSource{Error: stsError}, nil)

	o := oidc_mocks.NewMockProvider(ctrl)
	o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
		Return("some-id-token", nil)

	provider := &AwsTemporaryCredsProvider{
		OidcProvider:      o,
		CredentialsClient: a,
	}

	_, err := provider.ExchangeOauthToken(context.Background(), "some-org", "some-runner", testStsRegion, platformorchestratorcp.AwsTemporaryAuth{
		RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
		StsRegion: &stsRegion,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token")

	// Verify the error is classified as UserError
	var ue *usererrors.UserError
	assert.ErrorAs(t, err, &ue, "error should be classified as UserError")
}

func TestAwsTemporaryCredsProvider_GenericSTSError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stsRegion := testStsRegion

	// Simulate generic STS error with 4xx status code
	stsError := errors.New("operation error STS: AssumeRoleWithWebIdentity, https response error StatusCode: 400, RequestID: a0299db3-1404-4691-910a-57930516d1e3")

	a := aws_mocks.NewMockCredentialsClientInterface(ctrl)
	a.EXPECT().GetAssumeRoleTokenSource(gomock.Any(), "arn:aws:iam::123456789012:role/TestRole", "some-org-some-runner", "some-id-token", stsRegion).
		Return(&awsErrorTokenSource{Error: stsError}, nil)

	o := oidc_mocks.NewMockProvider(ctrl)
	o.EXPECT().CreateToken(gomock.Any(), "some-org+some-runner", "sts.amazonaws.com").
		Return("some-id-token", nil)

	provider := &AwsTemporaryCredsProvider{
		OidcProvider:      o,
		CredentialsClient: a,
	}

	_, err := provider.ExchangeOauthToken(context.Background(), "some-org", "some-runner", testStsRegion, platformorchestratorcp.AwsTemporaryAuth{
		RoleArn:   "arn:aws:iam::123456789012:role/TestRole",
		StsRegion: &stsRegion,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token")

	// Verify the error is classified as UserError
	var ue *usererrors.UserError
	assert.ErrorAs(t, err, &ue, "error should be classified as UserError")
}

// awsErrorTokenSource is a helper type for testing AWS-specific STS errors
type awsErrorTokenSource struct {
	Error error
}

func (e *awsErrorTokenSource) Token() (*oauth2.Token, error) {
	// Simulate the actual behavior: check if it's an STS error and wrap as UserError
	if awsclient.IsAssumeRoleError(e.Error) {
		return nil, usererrors.NewUserError(e.Error.Error())
	}
	return nil, e.Error
}

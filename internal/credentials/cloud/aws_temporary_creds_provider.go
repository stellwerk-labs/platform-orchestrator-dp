package cloud

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"golang.org/x/oauth2"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

type AwsTemporaryCredsProvider struct {
	CredentialsClient aws.CredentialsClientInterface
	OidcProvider      oidc.Provider
}

func (a *AwsTemporaryCredsProvider) ExchangeOauthToken(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*oauth2.Token, error) {
	if a.OidcProvider == nil {
		return nil, errors.New("oidc provider not configured")
	}
	subject := parseSubject(orgId, runnerId)
	idToken, err := a.OidcProvider.CreateToken(ctx, subject, "sts.amazonaws.com")
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC ID token: %w", err)
	}

	roleArn := auth.RoleArn
	stsRegion := ref.DerefOr(auth.StsRegion, region)
	sessionName := parseSessionName(orgId, runnerId, auth.SessionName)
	if ts, err := a.CredentialsClient.GetAssumeRoleTokenSource(ctx, roleArn, sessionName, idToken, stsRegion); err != nil {
		return nil, fmt.Errorf("failed to get assume role token source: %w", err)
	} else if t, err := ts.Token(); err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	} else {
		return t, nil
	}
}

func (a *AwsTemporaryCredsProvider) ExchangeAwsCredentials(ctx context.Context, orgId, runnerId, region string, auth platformorchestratorcp.AwsTemporaryAuth) (*aws.AWSCredentials, error) {
	token, err := a.ExchangeOauthToken(ctx, orgId, runnerId, region, auth)
	if err != nil {
		return nil, err
	}
	extractedCredentials, err := extractAwsCredentials(token)
	if err != nil {
		return nil, err
	}
	return &extractedCredentials, nil
}

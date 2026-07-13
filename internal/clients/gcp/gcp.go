package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/externalaccount"
)

//go:generate go tool mockgen -destination mocks/gcp_mock.go -package=mocks github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/gcp CredentialsClientInterface,ClusterManagerClientInterface

const (
	GcpScopeAuthCloudPlatform = "https://www.googleapis.com/auth/cloud-platform"
	GcpScopeAuthUserinfoEmail = "https://www.googleapis.com/auth/userinfo.email"
)

type GetTokenResponse struct {
	AccessToken string
	Expiry      time.Time
}

type GetAccessTokenInfo struct {
	Principal string
}

type CredentialsClientInterface interface {
	GetExternalAccountTokenSource(ctx context.Context, audience, serviceAccount, idToken string, additionalScopes []string) (oauth2.TokenSource, error)
	GetAccessTokenInfo(ctx context.Context, accessToken string) (*GetAccessTokenInfo, error)
}

type CredentialsClient struct {
	httpClient *http.Client
}

func (c *CredentialsClient) GetExternalAccountTokenSource(ctx context.Context, audience, serviceAccount, idToken string, additionalScopes []string) (oauth2.TokenSource, error) {
	config := externalaccount.Config{ //nolint:gosec
		Audience:             audience,
		SubjectTokenType:     "urn:ietf:params:oauth:token-type:id_token",
		TokenURL:             "https://sts.googleapis.com/v1/token",
		SubjectTokenSupplier: &subjectTokenSupplier{idToken: idToken},
		Scopes:               append([]string{GcpScopeAuthCloudPlatform, GcpScopeAuthUserinfoEmail}, additionalScopes...),
		ServiceAccountImpersonationURL: fmt.Sprintf(
			"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
			serviceAccount,
		),
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, c.httpClient)
	tokenSource, err := externalaccount.NewTokenSource(ctx, config)
	if err != nil {
		return nil, err
	}
	return tokenSource, nil
}

func NewCredentialsClient(hc *http.Client) CredentialsClientInterface {
	return &CredentialsClient{
		httpClient: hc,
	}
}

type subjectTokenSupplier struct {
	idToken string
}

func (s *subjectTokenSupplier) SubjectToken(_ context.Context, _ externalaccount.SupplierOptions) (string, error) {
	return s.idToken, nil
}

func (c *CredentialsClient) GetAccessTokenInfo(ctx context.Context, accessToken string) (*GetAccessTokenInfo, error) {
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, (&url.URL{
		Scheme:   "https",
		Host:     "oauth2.googleapis.com",
		Path:     "/tokeninfo",
		RawQuery: url.Values{"access_token": []string{accessToken}}.Encode(),
	}).String(), nil)
	if res, err := c.httpClient.Do(r); err != nil {
		return nil, errors.Wrap(err, "failed to make token info request")
	} else {
		defer func() {
			_ = res.Body.Close()
		}()
		var out struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			return nil, errors.Wrap(err, "failed to decode token info response")
		}
		return &GetAccessTokenInfo{
			Principal: out.Email,
		}, nil
	}
}

// ClusterManagerClientInterface for mocking out of the GCP API
type ClusterManagerClientInterface interface {
	GetCluster(context.Context, *containerpb.GetClusterRequest, ...gax.CallOption) (*containerpb.Cluster, error)
	Close() error
}

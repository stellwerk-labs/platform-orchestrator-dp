package api

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
)

// GetJwks Show JSON Web Keys Set (JWKS).
// (GET /.well-known/jwks)
func (s *Server) GetJwks(ctx context.Context, _ GetJwksRequestObject) (GetJwksResponseObject, error) {
	middleware.SetAuthCheckedCtx(ctx)
	if s.OidcIssuerUrl == "" {
		// If oidc is not configured, return 404
		return GetJwks404JSONResponse{}, nil
	}
	oidcProvider := oidc.NewProvider(s.OidcIssuerUrl, s.Vault, oidc.ProviderOptions{})
	jwksBytes, err := oidcProvider.GetJwks(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch jwks")
	}

	jwks := make([]Jwk, 0)
	for _, raw := range jwksBytes {
		var jwk Jwk
		if err = json.Unmarshal(raw, &jwk); err != nil {
			return nil, err
		}
		jwks = append(jwks, jwk)
	}
	return GetJwks200JSONResponse{Keys: jwks}, nil
}

// GetOpenidConfiguration Show OIDC discovery configuration.
// (GET /.well-known/openid-configuration)
func (s *Server) GetOpenidConfiguration(ctx context.Context, _ GetOpenidConfigurationRequestObject) (GetOpenidConfigurationResponseObject, error) {
	middleware.SetAuthCheckedCtx(ctx)
	if s.OidcIssuerUrl == "" {
		// If oidc is not configured, return 404
		return GetOpenidConfiguration404JSONResponse{}, nil
	}
	return GetOpenidConfiguration200JSONResponse{
		Issuer:                 s.OidcIssuerUrl,
		JwksUri:                s.OidcIssuerUrl + "/.well-known/jwks",
		SubjectTypesSupported:  []string{"public", "pairwise"},
		ResponseTypesSupported: []string{"id_token"},
		ClaimsSupported: []string{
			"sub",
			"aud",
			"exp",
			"iat",
			"iss",
			"jti",
			"nbf",
		},
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
		ScopesSupported:                  []string{"openid"},
	}, nil
}

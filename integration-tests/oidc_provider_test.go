package integrationtests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOpenidConfiguration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	dpClient := MustOidcProviderClient(t)
	resp, err := dpClient.GetOpenidConfigurationWithResponse(ctx)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())
	assert.Equal(t, []string{"public", "pairwise"}, resp.JSON200.SubjectTypesSupported)
	assert.Equal(t, []string{"id_token"}, resp.JSON200.ResponseTypesSupported)
	assert.Equal(t, []string{
		"sub",
		"aud",
		"exp",
		"iat",
		"iss",
		"jti",
		"nbf",
	}, resp.JSON200.ClaimsSupported)
	assert.Equal(t, []string{"RS256"}, resp.JSON200.IdTokenSigningAlgValuesSupported)
	assert.Equal(t, []string{"openid"}, resp.JSON200.ScopesSupported)
}

func TestGetJwks(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	dpClient := MustOidcProviderClient(t)
	resp, err := dpClient.GetJwksWithResponse(ctx)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())

	require.Len(t, resp.JSON200.Keys, 1)
	assert.Equal(t, "RS256", resp.JSON200.Keys[0].Alg)
}

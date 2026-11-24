package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
)

func TestPipelineCallerRequiresAuthenticatedNonSystemActor(t *testing.T) {
	t.Parallel()
	actor := uuid.New()
	for _, authenticated := range []string{"", "invalid", uuid.Nil.String(), userid.InternalSystemUuid.String(), actor.String()} {
		t.Run(authenticated, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("From", actor.String())
			req = req.WithContext(context.WithValue(req.Context(), hecho.ContextKeyUserID, authenticated))
			res := httptest.NewRecorder()
			c := echo.New().NewContext(req, res)
			err := middleware.EchoCtxMiddleware(func(c echo.Context) error {
				got, err := pipelineCaller(c.Request().Context())
				if authenticated == actor.String() {
					require.NoError(t, err)
					require.Equal(t, actor, got)
					return nil
				}
				var httpError *echo.HTTPError
				require.ErrorAs(t, err, &httpError)
				require.Equal(t, http.StatusUnauthorized, httpError.Code)
				require.Equal(t, uuid.Nil, got)
				return nil
			})(c)
			require.NoError(t, err)
			require.Equal(t, "no-store", res.Header().Get("Cache-Control"))
		})
	}
}

func TestPipelineCallerDoesNotAuthenticateFromRawHeader(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("From", uuid.NewString())
	_, err := pipelineCaller(request.Context())
	var denied *echo.HTTPError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, http.StatusUnauthorized, denied.Code)
}

func TestPipelinePlanBodyLimitScopeIsExact(t *testing.T) {
	t.Parallel()
	for _, trial := range []struct {
		method, path string
		skip         bool
	}{
		{http.MethodPost, "/orgs/a/deployments/b/pipeline/actions/apply-plan", false},
		{http.MethodPost, "/orgs/a/deployments/b/pipeline/controls", false},
		{http.MethodGet, "/orgs/a/deployments/b/pipeline/controls", true},
		{http.MethodPost, "/orgs/a/deployments/b/pipeline/controls/extra", true},
		{http.MethodPost, "/orgs/a%2Fb/deployments/b/pipeline/actions/replan", false},
		{http.MethodGet, "/orgs/a/deployments/b/pipeline/actions/replan", true},
		{http.MethodPost, "/orgs/a/deployments/b/pipeline/actions/replan/extra", true},
		{http.MethodPost, "/orgs/a/deployments/b/pipeline/actions/replan-other", true},
		{http.MethodPost, "/orgs/a/deployments", true},
		{http.MethodPost, "/orgs/a/deployments/b/pipeline", true},
		{http.MethodPost, "/orgs//deployments/b/pipeline/actions/replan", true},
	} {
		c := echo.New().NewContext(httptest.NewRequest(trial.method, trial.path, nil), httptest.NewRecorder())
		require.Equal(t, trial.skip, pipelinePlanBodyLimitSkipper(c), trial.path)
	}
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/google/uuid"
	platformorchestratorcpv2 "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
)

var (
	ErrExternalDeploymentUnauthorized = errors.New("external deployment grant was not authorized")
	ErrExternalDeploymentUnavailable  = errors.New("external deployment grant service is unavailable")
)

const (
	maxExternalDeploymentGrantResponseBytes = 16 * 1024
	// ExternalDeploymentGrantTimeout bounds the DP service-user callback through the Core gateway.
	ExternalDeploymentGrantTimeout = 5 * time.Second
)

type CoreExternalDeploymentGrantClient struct {
	BaseURL     *url.URL
	HTTPClient  *http.Client
	BearerToken string
}

func (c CoreExternalDeploymentGrantClient) AuthorizeExternalDeployment(
	ctx context.Context,
	claims plugincontext.Claims,
	request ExternalDeploymentGrantRequest,
) (AuthorizedExternalDeployment, error) {
	if c.BaseURL == nil || c.HTTPClient == nil || c.BearerToken == "" {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: Core grant client is not configured", ErrExternalDeploymentUnavailable)
	}
	if !validExternalPathIdentity(claims.OrgID, claims.PluginID) {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: invalid operation owner", ErrExternalDeploymentUnauthorized)
	}
	operationID, err := uuid.Parse(request.OperationRef)
	if err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: invalid operation reference", ErrExternalDeploymentUnauthorized)
	}
	requestBody := platformorchestratorcpv2.PluginDispatchCheck{
		Fence:           request.Fence,
		Action:          platformorchestratorcpv2.PluginDispatchCheckAction(request.ServiceAction),
		DispatchId:      request.DispatchID,
		ProjectUuid:     request.ProjectUUID,
		EnvironmentUuid: request.EnvUUID,
		RequestDigest:   request.RequestDigest,
	}
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("encode Core grant check: %w", err)
	}

	endpoint := *c.BaseURL
	endpoint.Path = path.Join(endpoint.Path, "orgs", claims.OrgID, "plugins", claims.PluginID, "operations", operationID.String(), "check")
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("build Core grant check: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+c.BearerToken)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	response, err := c.HTTPClient.Do(httpRequest)
	if err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: %v", ErrExternalDeploymentUnavailable, err)
	}
	defer func() { _ = response.Body.Close() }()

	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict:
		return AuthorizedExternalDeployment{}, ErrExternalDeploymentUnauthorized
	default:
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: Core returned status %d", ErrExternalDeploymentUnavailable, response.StatusCode)
	}

	limited := io.LimitReader(response.Body, maxExternalDeploymentGrantResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxExternalDeploymentGrantResponseBytes {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: invalid Core response", ErrExternalDeploymentUnavailable)
	}
	var checked platformorchestratorcpv2.PluginDispatchAuthorization
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checked); err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: invalid Core response", ErrExternalDeploymentUnavailable)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return AuthorizedExternalDeployment{}, fmt.Errorf("%w: invalid Core response", ErrExternalDeploymentUnavailable)
	}
	if checked.OperationId != operationID || checked.Fence != request.Fence ||
		checked.Action != request.ServiceAction || checked.DispatchId != request.DispatchID ||
		checked.PluginId != claims.PluginID || checked.OrgId != claims.OrgID ||
		checked.ProjectUuid != request.ProjectUUID || checked.EnvironmentUuid != request.EnvUUID ||
		checked.RequestDigest != request.RequestDigest || checked.ActorId.String() != claims.ActorID {
		return AuthorizedExternalDeployment{}, ErrExternalDeploymentUnauthorized
	}
	return AuthorizedExternalDeployment{
		OperationRef:  checked.OperationId.String(),
		Fence:         checked.Fence,
		ServiceAction: checked.Action,
		DispatchID:    checked.DispatchId,
		RequestDigest: checked.RequestDigest,
		ActorID:       checked.ActorId.String(),
		ProjectUUID:   checked.ProjectUuid,
		EnvUUID:       checked.EnvironmentUuid,
	}, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

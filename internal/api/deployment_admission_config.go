package api

import (
	"errors"
	"io"
	"net/http"
	"net/url"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const maxDeploymentAdmissionResponseBytes = 64 * 1024

var errDeploymentAdmissionResponseTooLarge = errors.New("core Deployment admission response is too large")

func NewOrdinaryDeploymentAdmissionService(database model.Databaser, rawCoreURL, bearerTokenFile string, baseHTTPClient *http.Client) (*OrdinaryDeploymentAdmissionService, error) {
	coreURL, err := url.Parse(rawCoreURL)
	if err != nil || (coreURL.Scheme != "http" && coreURL.Scheme != "https") || coreURL.Host == "" ||
		coreURL.User != nil || coreURL.RawQuery != "" || coreURL.Fragment != "" {
		return nil, errors.New("Deployment admission Core gateway URL is invalid")
	}
	bearerToken, err := LoadServiceBearerToken(bearerTokenFile)
	if err != nil {
		return nil, err
	}
	if baseHTTPClient == nil {
		baseHTTPClient = http.DefaultClient
	}
	httpClient := *baseHTTPClient
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = deploymentAdmissionResponseLimitTransport{next: transport}
	httpClient.Timeout = DeploymentAdmissionTimeout
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client, err := platformorchestratorcp.NewClientWithResponses(coreURL.String(), platformorchestratorcp.WithHTTPClient(&httpClient))
	if err != nil {
		return nil, err
	}
	return &OrdinaryDeploymentAdmissionService{Database: database, Core: client, BearerToken: bearerToken}, nil
}

type deploymentAdmissionResponseLimitTransport struct {
	next http.RoundTripper
}

func (t deploymentAdmissionResponseLimitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	response.Body = &deploymentAdmissionResponseBody{ReadCloser: response.Body, remaining: maxDeploymentAdmissionResponseBytes + 1}
	return response, nil
}

type deploymentAdmissionResponseBody struct {
	io.ReadCloser
	remaining int64
}

func (b *deploymentAdmissionResponseBody) Read(buffer []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, errDeploymentAdmissionResponseTooLarge
	}
	if int64(len(buffer)) > b.remaining {
		buffer = buffer[:b.remaining]
	}
	read, err := b.ReadCloser.Read(buffer)
	b.remaining -= int64(read)
	if b.remaining == 0 {
		return read, errDeploymentAdmissionResponseTooLarge
	}
	return read, err
}

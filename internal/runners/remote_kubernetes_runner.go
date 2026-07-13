package runners

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/genclient"
)

var dpHttpClient = &http.Client{
	Timeout:   time.Minute,
	Transport: otelhttp.NewTransport(http.DefaultTransport),
}

type ServiceDiscovery interface {
	LookupIP() ([]net.IP, error)
	GetPort() string
}

// defaultServiceDiscovery implements ServiceDiscovery using net.LookupIP
type defaultServiceDiscovery struct {
	host string
	port string
}

func (r *defaultServiceDiscovery) LookupIP() ([]net.IP, error) {
	return net.LookupIP(r.host)
}

func (r *defaultServiceDiscovery) GetPort() string {
	return r.port
}

type RemoteKubernetesRunner struct {
	externalDataplaneUrl      string
	runnerImage               string
	runnerTokenSalt           string
	logger                    *zap.Logger
	internalRunner            platformorchestratorcp.InternalRunner
	deploymentSummary         *model.DeploymentSummary
	dnsResolver               ServiceDiscovery
	runnerLogsBucketSignedUrl string
	podSchedulingDelay        time.Duration
}

func NewRemoteKubernetesRunner(
	externalDataplaneUrl string,
	runnerImage string,
	runnerTokenSalt string,
	logger *zap.Logger,
	internalRunner platformorchestratorcp.InternalRunner,
	deploymentSummary *model.DeploymentSummary,
	internalDataplaneHostname string,
	runnerLogsBucketSignedUrl string,
	podSchedulingDelay time.Duration,
) *RemoteKubernetesRunner {
	return &RemoteKubernetesRunner{
		externalDataplaneUrl: externalDataplaneUrl,
		runnerImage:          runnerImage,
		runnerTokenSalt:      runnerTokenSalt,
		logger:               logger,
		internalRunner:       internalRunner,
		deploymentSummary:    deploymentSummary,
		dnsResolver: &defaultServiceDiscovery{
			port: "8080",
			host: internalDataplaneHostname,
		},
		runnerLogsBucketSignedUrl: runnerLogsBucketSignedUrl,
		podSchedulingDelay:        podSchedulingDelay,
	}
}

func (r *RemoteKubernetesRunner) Start(ctx context.Context) error {
	jobCfg, err := getJobConfiguration(r.internalRunner.RunnerConfiguration)
	if err != nil {
		return errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	token := util.GenerateHashedRunnerToken(r.runnerTokenSalt, r.deploymentSummary.OrgId, r.deploymentSummary.Id.String())
	jobSpec, err := kubernetes.GetJobSpec(ctx, jobCfg, r.externalDataplaneUrl, r.runnerImage, token, r.runnerLogsBucketSignedUrl, "", r.deploymentSummary)
	if err != nil {
		return errors.Wrap(err, "failed to build job spec")
	}
	jobSpecJson, _ := json.Marshal(jobSpec)
	var jobSpecMap map[string]interface{}
	if err := json.Unmarshal(jobSpecJson, &jobSpecMap); err != nil {
		return errors.Wrap(err, "failed to unmarshal job spec JSON")
	}

	createJobMessage := new(serverclient.RemoteRunnerMessage)
	if err := createJobMessage.FromRemoteRunnerMessageCreateJob(serverclient.RemoteRunnerMessageCreateJob{
		Action:          serverclient.CreateJob,
		JobId:           r.deploymentSummary.Id.String(),
		Namespace:       jobCfg.Namespace,
		Configuration:   jobSpecMap,
		DeploymentToken: token,
	}); err != nil {
		return errors.Wrap(err, "failed to create remote runner message")
	}

	if err := r.sendMessageToRemoteRunner(ctx, createJobMessage); err != nil {
		return errors.Wrapf(ErrKubernetesAgentNotReachableRetry, "failed to send remote runner message but connection tolerance not exceeded yet: %v", err)
	}

	return nil
}

func (r *RemoteKubernetesRunner) IsRunning(ctx context.Context) (bool, error) {
	// For remote runners, we don't have a direct way to check if they're running
	// This would need to be implemented with a proper status check mechanism
	return false, nil
}

func (r *RemoteKubernetesRunner) CheckStatus(ctx context.Context) (*RunnerStatus, error) {
	jobCfg, err := getJobConfiguration(r.internalRunner.RunnerConfiguration)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	token := util.GenerateHashedRunnerToken(r.runnerTokenSalt, r.deploymentSummary.OrgId, r.deploymentSummary.Id.String())

	checkJobStatusMessage := new(serverclient.RemoteRunnerMessage)
	if err := checkJobStatusMessage.FromRemoteRunnerMessageCheckJobStatus(serverclient.RemoteRunnerMessageCheckJobStatus{
		Action:          serverclient.CheckJobStatus,
		JobId:           r.deploymentSummary.Id.String(),
		Namespace:       jobCfg.Namespace,
		DeploymentToken: token,
		ExpiresAt:       r.deploymentSummary.CreatedAt.Add(r.podSchedulingDelay),
	}); err != nil {
		return nil, errors.Wrap(err, "failed to create remote runner message for job status")
	}

	if err := r.sendMessageToRemoteRunner(ctx, checkJobStatusMessage); err != nil {
		return nil, errors.Wrapf(ErrKubernetesAgentNotReachableRetry, "failed to send remote runner message but connection tolerance not exceeded yet: %v", err)
	}
	return &RunnerStatus{
		IsCompleted: false,
		IsStuck:     false,
		Message:     "status check message sent to remote runner",
	}, nil
}

func (r *RemoteKubernetesRunner) sendMessageToRemoteRunner(ctx context.Context, message *serverclient.RemoteRunnerMessage) error {
	// Lookup service IPs
	ips, err := r.dnsResolver.LookupIP()
	if err != nil {
		r.logger.Error("failed to lookup service IPs", zap.Error(err))
		return errors.Wrap(err, "failed to lookup service IPs")
	} else if len(ips) == 0 {
		r.logger.Error("no IPs found for service", zap.String("host", r.dnsResolver.(*defaultServiceDiscovery).host))
		return errors.New("no IPs found for service")
	}
	var success bool
	for _, ip := range ips {
		serverURL := (&url.URL{Scheme: "http", Host: net.JoinHostPort(ip.String(), r.dnsResolver.GetPort())}).String()
		client, err := serverclient.NewClientWithResponses(serverURL, serverclient.WithHTTPClient(dpHttpClient))
		if err != nil {
			return errors.Wrap(err, "failed to create client for remote runner")
		}

		if res, err := client.InternalPushMessageToRemoteRunnerWithResponse(ctx, r.deploymentSummary.OrgId, r.deploymentSummary.RunnerId, *message); err != nil {
			r.logger.Warn("failed to push message to remote runner", zap.Error(err), zap.String("server", serverURL))
			continue
		} else if res.StatusCode() == http.StatusOK || res.StatusCode() == http.StatusNoContent {
			success = true
			r.logger.Info("successfully pushed message to remote runner", zap.String("server", serverURL))
			break
		} else {
			r.logger.Warn("unexpected status code from remote runner", zap.String("server", serverURL), zap.Int("status", res.StatusCode()))
			continue
		}
	}

	if !success {
		r.logger.Warn("failed to push message to all resolved IPs")
		return errors.New("failed to push message to all resolved IPs")
	} else {
		return nil
	}
}

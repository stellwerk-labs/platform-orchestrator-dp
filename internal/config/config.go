package config

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Configuration ...
type Configuration struct {
	Port int `env:"PORT, default=8080" validate:"required"`

	DatabaseName     string `env:"DATABASE_NAME"     validate:"required"`
	DatabaseUser     string `env:"DATABASE_USER"     validate:"required"`
	DatabasePassword string `env:"DATABASE_PASSWORD" validate:"required"`
	DatabaseHost     string `env:"DATABASE_HOST"     validate:"required"`
	DatabasePort     string `env:"DATABASE_PORT"     validate:"required"`

	// AmqpConnectionString should be an AMQP url like "amqp://%s:%s@%s:%d/%s"
	AmqpConnectionString string `env:"AMQP_CONNECTION_STRING" validate:"omitempty,url"`

	// Alternatively, separate env vars can be set for AMQP connection
	AmpqHost     string `env:"AMQP_HOST"`
	AmpqPort     string `env:"AMQP_PORT, default=5672"`
	AmpqVhost    string `env:"AMQP_VHOST"`
	AmpqUsername string `env:"AMQP_USERNAME"`
	AmpqPassword string `env:"AMQP_PASSWORD"`

	// ControlPlaneUrl is the api url for the control plane port 8080
	ControlPlaneUrl string `env:"CONTROL_PLANE_URL" validate:"required,url"`
	RunnerTokenSalt string `env:"RUNNER_TOKEN_SALT" validate:"required"`

	// IamUrl is the internal url for the platform-orchestrator-iam service
	IamUrl string `env:"IAM_URL" validate:"required,url"`

	// RunnerLogsBucket is the bucket for storing runner logs
	RunnerLogsBucket string `env:"RUNNER_LOGS_BUCKET" validate:"required"`

	// RunnerLogsBucketEndpoint is the endpoint for the object storage service (only needed for S3-compatible services)
	RunnerLogsBucketEndpoint string `env:"RUNNER_LOGS_BUCKET_ENDPOINT" validate:"omitempty"`

	RunnerLogsSignedUrlExpirationTime time.Duration `env:"RUNNER_LOGS_SIGNED_URL_EXPIRATION_TIME, default=1h"`
	RunnerLogsGCPServiceAccount       string        `env:"RUNNER_LOGS_GCP_SERVICE_ACCOUNT"`
	// RunnerLogsCreds used by integration tests
	RunnerLogsBucketCreds string `env:"RUNNER_LOGS_BUCKET_CREDS,default="`

	ShutdownDelay time.Duration `env:"SHUTDOWN_DELAY, default=10s"`
	OTELEnabled   bool          `env:"OTEL_ENABLE, default=false"`
	LogLevel      string        `env:"LOG_LEVEL, default=INFO"`

	DeploymentsCompletedBefore time.Duration `env:"DEPLOYMENTS_COMPLETED_BEFORE,default=5m"`

	RunnerImage string `env:"RUNNER_IMAGE"`
	// KubernetesRunnerPodSchedulingDelay is the time we wait for a k8s pod to be scheduled before marking the deployment as failed
	KubernetesRunnerPodSchedulingDelay time.Duration `env:"K8S_RUNNER_POD_SCHEDULING_DELAY, default=5m"`

	ExternalDataplaneUrl string `env:"SERVER_BASE_URL" validate:"url"`

	VaultURL  string `env:"VAULT_URL"  validate:"url"`
	VaultAuth string `env:"VAULT_AUTH" validate:"required"`
	VaultRole string `env:"VAULT_ROLE"`

	OidcIssuerUrl string `env:"OIDC_ISSUER_URL" validate:"omitempty,url"`

	InternalDataplaneHostname string `env:"INTERNAL_DATAPLANE_HOSTNAME" validate:"required"`

	MetadataOutputKey string `env:"METADATA_KEY, default=platform_orchestrator_metadata"`
}

func (c *Configuration) GetAmqpConnectionString() (string, error) {
	if c.AmqpConnectionString != "" {
		return c.AmqpConnectionString, nil
	}
	if c.AmpqHost == "" {
		return "", errors.New("AMQP_HOST or AMQP_CONNECTION_STRING is not set")
	}
	if c.AmpqVhost == "" {
		return "", errors.New("AMQP_VHOST or AMQP_CONNECTION_STRING is not set")
	}
	if c.AmpqUsername == "" {
		return "", errors.New("AMQP_USERNAME or AMQP_CONNECTION_STRING is not set")
	}
	if c.AmpqPassword == "" {
		return "", errors.New("AMQP_PASSWORD or AMQP_CONNECTION_STRING is not set")
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%s/%s", c.AmpqUsername, c.AmpqPassword, c.AmpqHost, c.AmpqPort, c.AmpqVhost), nil
}

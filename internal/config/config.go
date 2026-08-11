package config

import "time"

// Configuration ...
type Configuration struct {
	Port int `env:"PORT, default=8080" validate:"required"`

	DatabaseName     string `env:"DATABASE_NAME"     validate:"required"`
	DatabaseUser     string `env:"DATABASE_USER"     validate:"required"`
	DatabasePassword string `env:"DATABASE_PASSWORD" validate:"required"`
	DatabaseHost     string `env:"DATABASE_HOST"     validate:"required"`
	DatabasePort     string `env:"DATABASE_PORT"     validate:"required"`

	NATSURL            string `env:"NATS_URL"                        validate:"required,url"`
	NATSCredsFile      string `env:"NATS_CREDS_FILE"`
	NATSToken          string `env:"NATS_TOKEN"`
	NATSCAFile         string `env:"NATS_CA_FILE"`
	NATSClientCertFile string `env:"NATS_CLIENT_CERT_FILE"`
	NATSClientKeyFile  string `env:"NATS_CLIENT_KEY_FILE"`
	NATSStreamReplicas int    `env:"NATS_STREAM_REPLICAS, default=1" validate:"gte=1"`
	NATSBootstrap      bool   `env:"NATS_BOOTSTRAP, default=false"`

	// RunnerGatewayURL is the externally reachable HTTPS endpoint injected into
	// runners outside the data-plane cluster, such as ECS tasks.
	RunnerGatewayURL string `env:"RUNNER_GATEWAY_URL" validate:"required,url"`
	// RunnerGatewayInternalURL is used by Kubernetes Jobs created inside the
	// data-plane cluster. It may use cluster DNS and plain HTTP within the trust
	// boundary. When unset, RunnerGatewayURL is used for compatibility.
	RunnerGatewayInternalURL string `env:"RUNNER_GATEWAY_INTERNAL_URL" validate:"omitempty,url"`

	// ControlPlaneUrl is the api url for the control plane port 8080
	ControlPlaneUrl string `env:"CONTROL_PLANE_URL" validate:"required,url"`
	RunnerTokenSalt string `env:"RUNNER_TOKEN_SALT" validate:"required"`

	// IamUrl is the internal url for the platform-orchestrator-iam service
	IamUrl string `env:"IAM_URL" validate:"required,url"`

	ShutdownDelay time.Duration `env:"SHUTDOWN_DELAY, default=10s"`
	OTELEnabled   bool          `env:"OTEL_ENABLE, default=false"`
	LogLevel      string        `env:"LOG_LEVEL, default=INFO"`

	DeploymentsCompletedBefore time.Duration `env:"DEPLOYMENTS_COMPLETED_BEFORE,default=5m"`

	RunnerImage string `env:"RUNNER_IMAGE"`
	// KubernetesRunnerPodSchedulingDelay is the time we wait for a k8s pod to be scheduled before marking the deployment as failed
	KubernetesRunnerPodSchedulingDelay time.Duration `env:"K8S_RUNNER_POD_SCHEDULING_DELAY, default=5m"`
	RunnerCommandTTL                   time.Duration `env:"RUNNER_COMMAND_TTL, default=24h"             validate:"gt=0"`

	VaultURL  string `env:"VAULT_URL"  validate:"url"`
	VaultAuth string `env:"VAULT_AUTH" validate:"required"`
	VaultRole string `env:"VAULT_ROLE"`

	OidcIssuerUrl string `env:"OIDC_ISSUER_URL" validate:"omitempty,url"`

	MetadataOutputKey string `env:"METADATA_KEY, default=platform_orchestrator_metadata"`
}

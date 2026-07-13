package config

import (
	"os"
	"testing"
	"time"

	"github.com/stellwerk-labs/golib/hconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigLoading_CheckDefault(t *testing.T) {
	cfg := &Configuration{}
	_ = os.Setenv("DATABASE_NAME", "test_db")
	_ = os.Setenv("DATABASE_USER", "test_user")
	_ = os.Setenv("DATABASE_PASSWORD", "test_password")
	_ = os.Setenv("DATABASE_HOST", "localhost")
	_ = os.Setenv("DATABASE_PORT", "5432")
	_ = os.Setenv("CONTROL_PLANE_URL", "http://control-plane:8080")
	_ = os.Setenv("IAM_URL", "http://iam:8080")
	_ = os.Setenv("VAULT_URL", "http://vault:8200")
	_ = os.Setenv("VAULT_AUTH", "token")
	_ = os.Setenv("OIDC_ISSUER_URL", "http://oidc-issuer:8080")
	_ = os.Setenv("RUNNER_TOKEN_SALT", "test_salt")
	_ = os.Setenv("SERVER_BASE_URL", "http://server:8080")
	_ = os.Setenv("INTERNAL_DATAPLANE_HOSTNAME", "platform-orchestrator-dp-headless.platform-orchestrator-platform-v2.svc.cluster.local")
	_ = os.Setenv("RUNNER_LOGS_BUCKET", "runner-logs")
	err := hconfig.LoadConfigWithoutRetag(cfg)
	require.NoError(t, err, "Failed to load configuration")
	assert.Equal(t, &Configuration{
		Port:                               8080,
		DatabaseName:                       "test_db",
		DatabaseUser:                       "test_user",
		DatabasePassword:                   "test_password",
		DatabaseHost:                       "localhost",
		DatabasePort:                       "5432",
		AmqpConnectionString:               "",
		AmpqPort:                           "5672",
		ControlPlaneUrl:                    "http://control-plane:8080",
		RunnerTokenSalt:                    "test_salt",
		ShutdownDelay:                      10 * time.Second, // 10 seconds
		OTELEnabled:                        false,
		LogLevel:                           "INFO",
		DeploymentsCompletedBefore:         5 * time.Minute, // 5 minutes
		RunnerImage:                        "",
		ExternalDataplaneUrl:               "http://server:8080",
		IamUrl:                             "http://iam:8080",
		VaultURL:                           "http://vault:8200",
		VaultAuth:                          "token",
		OidcIssuerUrl:                      "http://oidc-issuer:8080",
		InternalDataplaneHostname:          "platform-orchestrator-dp-headless.platform-orchestrator-platform-v2.svc.cluster.local",
		RunnerLogsBucket:                   "runner-logs",
		RunnerLogsSignedUrlExpirationTime:  1 * time.Hour,
		KubernetesRunnerPodSchedulingDelay: 5 * time.Minute,
		RunnerLogsBucketCreds:              "",
		MetadataOutputKey:                  "platform_orchestrator_metadata",
	}, cfg, "Configuration should match the expected default values")
}

func TestConfiguration_GetAmqpConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		config  Configuration
		want    string
		wantErr bool
	}{
		{ //nolint:gosec
			name: "AmqpConnectionString is set",
			config: Configuration{ //nolint:gosec
				AmqpConnectionString: "amqp://user:pass@host:5672/vhost",
			},
			want:    "amqp://user:pass@host:5672/vhost",
			wantErr: false,
		},
		{ //nolint:gosec
			name: "Individual fields are set",
			config: Configuration{
				AmpqHost:     "host",
				AmpqPort:     "5672",
				AmpqVhost:    "vhost",
				AmpqUsername: "user",
				AmpqPassword: "pass",
			},
			want:    "amqp://user:pass@host:5672/vhost",
			wantErr: false,
		},
		{
			name: "Missing Host",
			config: Configuration{
				AmpqPort:     "5672",
				AmpqVhost:    "vhost",
				AmpqUsername: "user",
				AmpqPassword: "pass",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing Vhost",
			config: Configuration{
				AmpqHost:     "host",
				AmpqPort:     "5672",
				AmpqUsername: "user",
				AmpqPassword: "pass",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing Username",
			config: Configuration{
				AmpqHost:     "host",
				AmpqPort:     "5672",
				AmpqVhost:    "vhost",
				AmpqPassword: "pass",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing Password",
			config: Configuration{
				AmpqHost:     "host",
				AmpqPort:     "5672",
				AmpqVhost:    "vhost",
				AmpqUsername: "user",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.config.GetAmqpConnectionString()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAmqpConnectionString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetAmqpConnectionString() got = %v, want %v", got, tt.want)
			}
		})
	}
}

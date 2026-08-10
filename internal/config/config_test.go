package config

import (
	"testing"
	"time"

	"github.com/stellwerk-labs/golib/hconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigLoading_CheckDefault(t *testing.T) {
	t.Setenv("DATABASE_NAME", "test_db")
	t.Setenv("DATABASE_USER", "test_user")
	t.Setenv("DATABASE_PASSWORD", "test_password")
	t.Setenv("DATABASE_HOST", "localhost")
	t.Setenv("DATABASE_PORT", "5432")
	t.Setenv("CONTROL_PLANE_URL", "http://control-plane:8080")
	t.Setenv("IAM_URL", "http://iam:8080")
	t.Setenv("VAULT_URL", "http://vault:8200")
	t.Setenv("VAULT_AUTH", "token")
	t.Setenv("OIDC_ISSUER_URL", "http://oidc-issuer:8080")
	t.Setenv("RUNNER_TOKEN_SALT", "test_salt")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("RUNNER_NATS_URL", "nats://runner-nats:4222")

	cfg := new(Configuration)
	require.NoError(t, hconfig.LoadConfigWithoutRetag(cfg))
	assert.Equal(t, &Configuration{
		Port:                               8080,
		DatabaseName:                       "test_db",
		DatabaseUser:                       "test_user",
		DatabasePassword:                   "test_password",
		DatabaseHost:                       "localhost",
		DatabasePort:                       "5432",
		NATSURL:                            "nats://nats:4222",
		NATSStreamReplicas:                 1,
		NATSBootstrap:                      false,
		RunnerNATSURL:                      "nats://runner-nats:4222",
		ControlPlaneUrl:                    "http://control-plane:8080",
		RunnerTokenSalt:                    "test_salt",
		ShutdownDelay:                      10 * time.Second,
		OTELEnabled:                        false,
		LogLevel:                           "INFO",
		DeploymentsCompletedBefore:         5 * time.Minute,
		RunnerImage:                        "",
		IamUrl:                             "http://iam:8080",
		VaultURL:                           "http://vault:8200",
		VaultAuth:                          "token",
		OidcIssuerUrl:                      "http://oidc-issuer:8080",
		KubernetesRunnerPodSchedulingDelay: 5 * time.Minute,
		RunnerCommandTTL:                   24 * time.Hour,
		MetadataOutputKey:                  "platform_orchestrator_metadata",
	}, cfg)
}

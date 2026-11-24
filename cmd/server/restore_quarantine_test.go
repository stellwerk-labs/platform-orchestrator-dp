package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if os.Getenv("STELLWERK_TEST_SERVER_PROCESS") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestRestoreQuarantineStopsBeforeConnectingToServices(t *testing.T) {
	for _, trial := range []struct {
		name, value, diagnostic string
	}{
		{"quarantined", "true", "restore quarantine prevents startup"},
		{"invalid", "invalid", "failed to load config"},
	} {
		t.Run(trial.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			connected := make(chan struct{}, 1)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				connection, acceptErr := listener.Accept()
				if acceptErr == nil {
					connected <- struct{}{}
					_ = connection.Close()
					_ = listener.Close()
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				<-stopped
			})
			binary, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			process := exec.CommandContext(ctx, binary) // #nosec G204 -- re-executes this test binary, never request input.
			process.Env = append(quarantineProcessEnvironment(t, listener.Addr().String()), "RESTORE_QUARANTINE="+trial.value)
			output, err := process.CombinedOutput()
			require.Error(t, err, "quarantine or invalid configuration must refuse startup")
			_ = listener.Close()
			<-stopped
			require.Empty(t, connected, "quarantine must precede every configured service connection")
			require.NoError(t, ctx.Err(), "server process must terminate within its deadline")
			require.Contains(t, string(output), trial.diagnostic)
		})
	}
}

func quarantineProcessEnvironment(t *testing.T, address string) []string {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	return []string{
		"STELLWERK_TEST_SERVER_PROCESS=1",
		"OTEL_ENABLE=true", "OTEL_EXPORTER_OTLP_ENDPOINT=http://" + address,
		"DATABASE_NAME=quarantine-test", "DATABASE_USER=fixture", "DATABASE_PASSWORD=fixture",
		"DATABASE_HOST=" + host, "DATABASE_PORT=" + port, "DATABASE_SSLMODE=disable",
		"NATS_URL=nats://" + address, "RUNNER_GATEWAY_URL=http://" + address,
		"CONTROL_PLANE_URL=http://" + address, "IAM_URL=http://" + address,
		"VAULT_URL=http://" + address, "VAULT_AUTH=token:fixture",
		"RUNNER_TOKEN_SALT=fixture",
	}
}

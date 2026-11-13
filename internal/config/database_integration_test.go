//go:build integration

package config

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The fixture needs PostgreSQL on localhost with a localhost-only server
// certificate, a trusted CA, an unrelated CA, and a second port with TLS off.
func TestDatabaseTLS(t *testing.T) {
	port := os.Getenv("DATABASE_TLS_TEST_PORT")
	if port == "" {
		t.Skip("set DATABASE_TLS_TEST_PORT, DATABASE_TLS_TEST_PLAINTEXT_PORT, DATABASE_TLS_TEST_CA and DATABASE_TLS_TEST_UNTRUSTED_CA")
	}
	plainPort := os.Getenv("DATABASE_TLS_TEST_PLAINTEXT_PORT")
	ca := os.Getenv("DATABASE_TLS_TEST_CA")
	untrustedCA := os.Getenv("DATABASE_TLS_TEST_UNTRUSTED_CA")
	require.NotEmpty(t, plainPort)
	require.NotEmpty(t, ca)
	require.NotEmpty(t, untrustedCA)
	for _, test := range []struct {
		name, mode, host, port, ca string
		tls                        bool
		wantError                  string
	}{
		{name: "disable", mode: "disable", host: "localhost", port: port},
		{name: "require", mode: "require", host: "localhost", port: port, tls: true},
		{name: "require refuses plaintext", mode: "require", host: "localhost", port: plainPort, wantError: "SSL is not enabled"},
		{name: "verify-ca", mode: "verify-ca", host: "localhost", port: port, ca: ca, tls: true},
		{name: "verify-full", mode: "verify-full", host: "localhost", port: port, ca: ca, tls: true},
		{name: "wrong CA", mode: "verify-full", host: "localhost", port: port, ca: untrustedCA, wantError: "unknown authority"},
		{name: "wrong hostname", mode: "verify-full", host: "127.0.0.1", port: port, ca: ca, wantError: "IP SANs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PGSSLROOTCERT", test.ca)
			configuration := Configuration{
				DatabaseName: "postgres", DatabaseUser: "postgres", DatabasePassword: "tls-test-only",
				DatabaseHost: test.host, DatabasePort: test.port, DatabaseSSLMode: test.mode,
			}
			db, err := sql.Open("postgres", configuration.DatabaseConnectionString())
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var encrypted bool
			err = db.QueryRowContext(ctx, "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").Scan(&encrypted)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.tls, encrypted)
		})
	}
}

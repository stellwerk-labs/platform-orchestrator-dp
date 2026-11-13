package config

import (
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestDatabaseConnectionStringPreservesValues(t *testing.T) {
	configuration := Configuration{
		DatabaseName: `db 'quoted' \ name`, DatabaseUser: `user 'quoted' \ name`,
		DatabasePassword: `secret 'quoted' \ value`, DatabaseHost: `db 'quoted' \ host`,
		DatabasePort: "5432", DatabaseSSLMode: "require",
	}
	parsed, err := pq.NewConfig(configuration.DatabaseConnectionString())
	require.NoError(t, err)
	require.Equal(t, configuration.DatabaseName, parsed.Database)
	require.Equal(t, configuration.DatabaseUser, parsed.User)
	require.Equal(t, configuration.DatabasePassword, parsed.Password)
	require.Equal(t, configuration.DatabaseHost, parsed.Host)
	require.EqualValues(t, 5432, parsed.Port)
	require.Equal(t, pq.SSLModeRequire, parsed.SSLMode)
}

func TestDatabaseConnectionStringSSLMode(t *testing.T) {
	for _, mode := range []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full", "invalid", "require sslmode=disable", ""} {
		t.Run(mode, func(t *testing.T) {
			configuration := Configuration{DatabaseHost: "localhost", DatabasePort: "5432", DatabaseSSLMode: mode}
			_, err := pq.NewConnector(configuration.DatabaseConnectionString())
			if mode == "invalid" || mode == "require sslmode=disable" || mode == "" {
				require.ErrorContains(t, err, "sslmode", "the driver must reject invalid modes before opening a connection")
				return
			}
			require.NoError(t, err)
		})
	}
}

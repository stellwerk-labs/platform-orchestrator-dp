package config

import (
	"fmt"
	"strings"
)

// DatabaseConnectionString returns the PostgreSQL connection string used by
// the server and its schema migrations. Certificate options use lib/pq's PGSSL* environment variables.
func (c Configuration) DatabaseConnectionString() string {
	quote := func(value string) string {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
	}
	return fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s connect_timeout=5 sslmode=%s",
		quote(c.DatabaseName), quote(c.DatabaseUser), quote(c.DatabasePassword),
		quote(c.DatabaseHost), quote(c.DatabasePort), quote(c.DatabaseSSLMode))
}

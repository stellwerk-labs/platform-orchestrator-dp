package runners

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBufferedCommandCannotOutliveFirstDispatchBudget(t *testing.T) {
	now := time.Now().UTC()
	deadline := now.Add(30 * time.Second)
	expiry, err := boundedRunnerCommandExpiry(now, time.Hour, deadline)
	require.NoError(t, err)
	require.Equal(t, deadline, expiry)
	expiry, err = boundedRunnerCommandExpiry(now, 5*time.Second, deadline)
	require.NoError(t, err)
	require.Equal(t, now.Add(5*time.Second), expiry)
	_, err = boundedRunnerCommandExpiry(deadline, time.Hour, deadline)
	require.ErrorContains(t, err, "exhausted")
}

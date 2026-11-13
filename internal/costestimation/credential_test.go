package costestimation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidAPIKey(t *testing.T) {
	for _, value := range []string{"", "fixture-key", strings.Repeat("a", 4096)} {
		require.True(t, ValidAPIKey(value))
	}
	for _, value := range []string{"a b", " key", "key\n", "a\tb", "key\x00", "key\u0085", strings.Repeat("a", 4097)} {
		require.False(t, ValidAPIKey(value))
	}
}

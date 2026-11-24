package bundles

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetainedBundleContainsOnlyCiphertext(t *testing.T) {
	ciphertext := []byte("age-encrypted-data")
	archive, err := BuildRetained(ciphertext)
	require.NoError(t, err)
	gz, err := gzip.NewReader(archive)
	require.NoError(t, err)
	reader := tar.NewReader(gz)
	header, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, ".stellwerk-retained-plan-v1.age", header.Name)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, ciphertext, got)
	_, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
}

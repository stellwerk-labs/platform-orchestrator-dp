package api

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/stretchr/testify/require"
)

func TestDecryptExternalDeploymentPayloadEnforcesPlaintextLimit(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	encode := func(plaintext string) string {
		var encrypted bytes.Buffer
		writer, encryptErr := age.Encrypt(&encrypted, identity.Recipient())
		require.NoError(t, encryptErr)
		_, encryptErr = writer.Write([]byte(plaintext))
		require.NoError(t, encryptErr)
		require.NoError(t, writer.Close())
		return base64.StdEncoding.EncodeToString(encrypted.Bytes())
	}

	payload, err := decryptExternalDeploymentPayload(strings.NewReader(encode("12345678")), identity, 8)
	require.NoError(t, err)
	require.Equal(t, "12345678", string(payload))

	_, err = decryptExternalDeploymentPayload(strings.NewReader(encode("123456789")), identity, 8)
	require.ErrorIs(t, err, errExternalMaterialTooLarge)

	wrongIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	_, err = decryptExternalDeploymentPayload(strings.NewReader(encode("secret")), wrongIdentity, 8)
	require.ErrorIs(t, err, errExternalMaterialInvalid)
}

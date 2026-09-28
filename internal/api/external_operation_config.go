package api

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxExternalDeploymentCredentialBytes = 16 * 1024

func LoadExternalDeploymentCredentials(publicKeyPath, bearerTokenPath string) (ed25519.PublicKey, string, error) {
	publicKeyData, err := readBoundedOperatorFile(publicKeyPath)
	if err != nil {
		return nil, "", fmt.Errorf("read external deployment verification key: %w", err)
	}
	block, rest := pem.Decode(publicKeyData)
	if block == nil || block.Type != "PUBLIC KEY" || len(rest) != 0 {
		return nil, "", errors.New("external deployment verification key must be one PKIX PEM public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, "", errors.New("external deployment verification key is invalid")
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return nil, "", errors.New("external deployment verification key must use Ed25519")
	}

	tokenData, err := readBoundedOperatorFile(bearerTokenPath)
	if err != nil {
		return nil, "", fmt.Errorf("read DP Core service bearer token: %w", err)
	}
	token := strings.TrimSpace(string(tokenData))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, "", errors.New("DP Core service bearer token must be one non-empty token")
	}
	return publicKey, token, nil
}

func readBoundedOperatorFile(name string) ([]byte, error) {
	if !filepath.IsAbs(name) {
		return nil, errors.New("operator credential files require an absolute path")
	}
	file, err := os.OpenInRoot(filepath.Dir(name), filepath.Base(name))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxExternalDeploymentCredentialBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxExternalDeploymentCredentialBytes {
		return nil, errors.New("operator credential file is too large")
	}
	return data, nil
}

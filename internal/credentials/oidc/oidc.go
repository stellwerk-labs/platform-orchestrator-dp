package oidc

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

//go:generate go tool mockgen -destination mocks/oidc_mock.go -package=mocks github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc Provider

const (
	KeyName             = "oidc-provider-key"
	KeyAutoRotatePeriod = "30d"

	DefaultTokenExpiredAfter = 1 * time.Hour
)

type Provider interface {
	GetJwks(ctx context.Context) ([][]byte, error)
	CreateToken(ctx context.Context, subject, audience string) (string, error)
}

type ProviderOptions struct {
	TokenExpireAfter time.Duration
}

func NewProvider(issuerUrl string, vaultClient vault.VaultClientInterface, opts ProviderOptions) Provider {
	tokenExpiredAfter := DefaultTokenExpiredAfter
	if opts.TokenExpireAfter > 0 {
		tokenExpiredAfter = opts.TokenExpireAfter
	}
	return &provider{
		issuerUrl:        issuerUrl,
		vault:            vaultClient,
		tokenExpireAfter: tokenExpiredAfter,
	}
}

type provider struct {
	issuerUrl        string
	vault            vault.VaultClientInterface
	tokenExpireAfter time.Duration
}

// ensureOIDCKey Ensures that a private/public key pair for OIDC provider exists and returns it
func (p *provider) ensureOIDCKey(ctx context.Context) (*vault.TransitKey, error) {
	vaultKey, err := p.vault.ReadKey(ctx, KeyName)
	if errors.Is(err, vault.ErrSecretNotFound) {
		if err = p.vault.CreateKey(ctx, KeyName, KeyAutoRotatePeriod); err != nil {
			return nil, err
		}
		vaultKey, err = p.vault.ReadKey(ctx, KeyName)
	}
	return vaultKey, err
}

// GetJwks returns JSON Web Keys Set (JWKS)
func (p *provider) GetJwks(ctx context.Context) ([][]byte, error) {
	vaultKey, err := p.ensureOIDCKey(ctx)
	if err != nil {
		return nil, err
	}

	jwks := make([][]byte, 0)
	// Return 2 last keys
	keys := slices.SortedFunc(maps.Values(vaultKey.Keys), func(a, b vault.TransitKeyVersion) int {
		return b.CreationTime.Compare(a.CreationTime) // reverse order
	})
	keysToReturn := 2
	for _, key := range keys[:min(len(keys), keysToReturn)] {
		timestamp := fmt.Sprintf("%d", key.CreationTime.Unix())
		joseJwk, err := jwkFromPEM(timestamp, key.PublicKey)
		if err != nil {
			return nil, err
		}
		raw, err := joseJwk.MarshalJSON()
		if err != nil {
			return nil, err
		}
		jwks = append(jwks, raw)
	}
	return jwks, nil
}

func (p *provider) CreateToken(ctx context.Context, subject, audience string) (string, error) {
	vaultKey, err := p.ensureOIDCKey(ctx)
	if err != nil {
		return "", err
	}

	pubKey := vaultKey.Keys[strconv.Itoa(vaultKey.LatestVersion)]

	now := time.Now().UTC()
	expired := now.Add(p.tokenExpireAfter)
	claims := map[string]interface{}{
		"iss": p.issuerUrl,
		"sub": subject,
		"aud": audience,
		"iat": now.Unix(),
		"nbf": now.Unix(),
		"exp": expired.Unix(),
	}
	headers := map[string]interface{}{
		"alg": "RS256",
		"typ": "JWT",
		"kid": fmt.Sprintf("%d", pubKey.CreationTime.Unix()),
	}

	jwtBase64URLEncoded, err := encodeJWTBase64URL(headers, claims)
	if err != nil {
		return "", err
	}
	input := base64.StdEncoding.EncodeToString([]byte(jwtBase64URLEncoded))

	signResp, err := p.vault.SignData(ctx, KeyName, input)
	if err != nil {
		return "", err
	}

	if signature, ok := signResp["signature"].(string); !ok {
		return "", errors.New("signing request doesn't return signature")
	} else {
		signature = signature[strings.LastIndex(signature, ":")+1:]
		return fmt.Sprintf("%s.%s", jwtBase64URLEncoded, signature), nil
	}
}

func jwkFromPEM(keyName, pemKey string) (*jose.JSONWebKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, fmt.Errorf("no public key found during PEM decode")
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return &jose.JSONWebKey{
		KeyID:     keyName,
		Key:       key,
		Algorithm: "RS256",
		Use:       "sig",
	}, nil
}

func encodeJWTBase64URL(headers, claims map[string]interface{}) (string, error) {
	headersByte, err := json.Marshal(headers)
	if err != nil {
		return "", errors.Wrap(err, "marshaling headers")
	}

	claimsByte, err := json.Marshal(claims)
	if err != nil {
		return "", errors.Wrap(err, "marshaling claims")
	}

	return fmt.Sprintf("%s.%s", base64.RawURLEncoding.EncodeToString(headersByte), base64.RawURLEncoding.EncodeToString(claimsByte)), nil
}

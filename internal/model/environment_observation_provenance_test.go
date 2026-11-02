package model

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stretchr/testify/require"
)

func TestObservationProvenanceAllowsOnlyFrozenSelectorsWithoutDigest(t *testing.T) {
	commit := strings.Repeat("a", 40)
	for _, test := range []struct {
		name, source string
		valid        bool
	}{
		{"inline", "inline", true},
		{"https full commit", "git::https://example.invalid/modules.git//storage?ref=" + commit, true},
		{"ssh full commit", "git::ssh://git@example.invalid/modules.git?ref=" + commit, true},
		{"sha256 commit", "git::https://example.invalid/modules.git?ref=" + strings.Repeat("b", 64), true},
		{"moving branch", "git::https://example.invalid/modules.git?ref=main", false},
		{"short commit", "git::https://example.invalid/modules.git?ref=abc123", false},
		{"uppercase commit", "git::https://example.invalid/modules.git?ref=" + strings.Repeat("A", 40), false},
		{"no executable selector", "git::https://example.invalid/modules.git", false},
		{"duplicate selector", "git::https://example.invalid/modules.git?ref=" + commit + "&ref=" + commit, false},
		{"extra shallow selector", "git::https://example.invalid/modules.git?ref=" + commit + "&depth=1", false},
		{"extra query", "git::https://example.invalid/modules.git?ref=" + commit + "&other=value", false},
		{"fragment", "git::https://example.invalid/modules.git?ref=" + commit + "#module", false},
		{"empty fragment", "git::https://example.invalid/modules.git?ref=" + commit + "#", false},
		{"password", "git::https://user:password@example.invalid/modules.git?ref=" + commit, false},
		{"empty password", "git::ssh://git:@example.invalid/modules.git?ref=" + commit, false},
		{"file transport", "git::file:///tmp/modules.git?ref=" + commit, false},
		{"http transport", "git::http://example.invalid/modules.git?ref=" + commit, false},
		{"plain url", "https://example.invalid/modules.git?ref=" + commit, false},
		{"descriptive full revision alone", commit, false},
		{"registry address", "namespace/name/provider", false},
		{"archive URL", "https://example.invalid/module.zip", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := ModuleArtifactRequirement{ModuleID: "example", Version: "1.0.0", ModuleUUID: uuid.New(), VersionUUID: uuid.New(), Source: test.source}
			require.Equal(t, test.valid, artifact.HasObservationProvenance())
			if test.valid {
				artifact.ModuleUUID = uuid.Nil
				require.False(t, artifact.HasObservationProvenance())
				artifact.ModuleUUID = uuid.New()
				artifact.VersionUUID = uuid.Nil
				require.False(t, artifact.HasObservationProvenance())
				artifact.VersionUUID = uuid.New()
				artifact.DraftTest = &graphs.DraftTestReference{}
				require.False(t, artifact.HasObservationProvenance())
			}
		})
	}
}

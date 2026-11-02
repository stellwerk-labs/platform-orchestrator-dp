package model

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// InlineModuleSource is the source kind whose bytes belong to an immutable Core Module Version.
const InlineModuleSource = "inline"

var observationFullGitCommit = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

// HasObservationProvenance requires exact retained immutable Module identity.
// A digest is optional for inline bytes or an executable full-commit Git pin;
// descriptive revision metadata does not freeze a source selector.
func (artifact ModuleArtifactRequirement) HasObservationProvenance() bool {
	return artifact.DraftTest == nil && artifact.ModuleID != "" && artifact.Version != "" && artifact.Source != "" &&
		artifact.ModuleUUID != uuid.Nil && artifact.VersionUUID != uuid.Nil &&
		(artifact.Source == InlineModuleSource || artifact.ArtifactDigest != "" || immutableObservationGitSource(artifact.Source))
}

// Match the executable Git selector policy used by Core Draft test publication,
// without treating a digest claim as proof of downloaded archive verification.
func immutableObservationGitSource(source string) bool {
	if !strings.HasPrefix(source, "git::") || source != strings.TrimSpace(source) || strings.Contains(source, "#") {
		return false
	}
	address, err := url.Parse(strings.TrimPrefix(source, "git::"))
	if err != nil || address.Host == "" || address.Path == "" || (address.Scheme != "https" && address.Scheme != "ssh") || address.Fragment != "" {
		return false
	}
	if address.User != nil {
		if _, password := address.User.Password(); password {
			return false
		}
	}
	query, err := url.ParseQuery(address.RawQuery)
	return err == nil && len(query) == 1 && len(query["ref"]) == 1 && observationFullGitCommit.MatchString(query.Get("ref"))
}

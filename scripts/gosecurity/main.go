// gosecurity preserves govulncheck's symbol-level gate while correcting two
// stale records for an exact, upstream-fixed Echo release.
package main

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
)

const binaryMode = "binary"
const sourceMode = "source"

const echoPath = "github.com/labstack/echo/v4"
const echoVersion = "v4.16.3"
const echoSum = "h1:wAzM49wiBxHpZJQ621lminvdxbwfd1Cn6KwRThKV6nA="

// Upstream v4.16.3 fixes both advisories in ebbb855f5827b76fb206e267dd91d020000baf66.
// https://github.com/labstack/echo/security/advisories/GHSA-246p-cpwv-v3jq
// https://github.com/labstack/echo/security/advisories/GHSA-99jh-6h7p-pp36
// These hashes bind the stale OSV records, not just their IDs. Updated records
// that still flag this version require a fresh review.
var correctedRecords = map[string]string{
	"GO-2026-6872": "3db56452379abef36e7ead8a843de96df3843ac872e3dc48f0954a48d7a4e26b",
	"GO-2026-6875": "790e8da3e93290555226abe7b9f80ad90cc7610689ac643dcb6c8db300a0e990",
}

type frame struct{ Module, Version, Package, Function string }
type finding struct {
	OSV   string
	Trace []frame
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 || (args[0] != sourceMode && args[0] != binaryMode) || (args[0] == sourceMode && len(args) != 1) || (args[0] == binaryMode && len(args) != 2) {
		return fmt.Errorf("usage: gosecurity source | binary PATH")
	}
	if args[0] == binaryMode {
		absolute, err := filepath.Abs(args[1])
		if err != nil {
			return err
		}
		if _, err := buildinfo.ReadFile(absolute); err != nil {
			return err
		}
		args[1] = absolute
	}
	scanner := []string{"run", "golang.org/x/vuln/cmd/govulncheck@v1.9.0", "-json"}
	if args[0] == binaryMode {
		scanner = append(scanner, "-mode=binary", args[1])
	} else {
		scanner = append(scanner, "./...")
	}
	// Fixed executable and scanner options; an absolute Go binary path cannot inject flags.
	command := exec.Command("go", scanner...) //nolint:gosec // G702: no shell or user-selected executable/options.
	command.Stderr = os.Stderr
	output, err := command.Output()
	// JSON mode returns zero for findings. Retain its complete raw report, then
	// explicitly enforce symbol-level findings below.
	if _, writeErr := os.Stdout.Write(output); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return fmt.Errorf("govulncheck failed: %w", err)
	}
	load := func() (*debug.Module, error) {
		if args[0] == binaryMode {
			info, err := buildinfo.ReadFile(args[1])
			if err != nil {
				return nil, err
			}
			for _, module := range info.Deps {
				if module.Path == echoPath {
					return module, nil
				}
			}
			return nil, fmt.Errorf("echo is absent from binary build information")
		}
		raw, err := exec.Command("go", "list", "-m", "-json", echoPath).Output()
		if err != nil {
			return nil, err
		}
		var module debug.Module
		err = json.Unmarshal(raw, &module)
		return &module, err
	}
	module, _ := load()
	return evaluate(output, args[0], module)
}

func evaluate(report []byte, mode string, module *debug.Module) error {
	decoder := json.NewDecoder(bytes.NewReader(report))
	decoder.UseNumber()
	records := map[string]string{}
	findings := []finding{}
	configured := false
	sbomCount := 0
	for {
		offset := decoder.InputOffset()
		value, err := readValue(decoder)
		if err == io.EOF && len(bytes.TrimSpace(report[offset:])) == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid govulncheck report: %w", err)
		}
		record, ok := value.(map[string]any)
		if !ok || len(record) != 1 {
			return fmt.Errorf("invalid govulncheck record")
		}
		for key, data := range record {
			if !configured && key != "config" {
				return fmt.Errorf("scanner configuration must be first")
			}
			if _, object := data.(map[string]any); !object {
				return fmt.Errorf("invalid %s payload", key)
			}
			raw, err := canonical(data)
			if err != nil {
				return err
			}
			switch key {
			case "config":
				if configured {
					return fmt.Errorf("duplicate scanner configuration")
				}
				var config struct {
					Protocol string `json:"protocol_version"`
					Name     string `json:"scanner_name"`
					Version  string `json:"scanner_version"`
					Level    string `json:"scan_level"`
					Mode     string `json:"scan_mode"`
					DB       string `json:"db"`
				}
				if err := json.Unmarshal(raw, &config); err != nil {
					return err
				}
				if config.Protocol != "v1.0.0" || config.Name != "govulncheck" || config.Version != "v1.9.0" || config.Level != "symbol" || config.Mode != mode || config.DB != "https://vuln.go.dev" {
					return fmt.Errorf("unsupported scanner configuration")
				}
				configured = true
			case "osv":
				var osv struct{ ID string }
				if err := json.Unmarshal(raw, &osv); err != nil || osv.ID == "" {
					return fmt.Errorf("invalid OSV record")
				}
				hash := fmt.Sprintf("%x", sha256.Sum256(raw))
				if previous, exists := records[osv.ID]; exists && previous != hash {
					return fmt.Errorf("conflicting OSV record %s", osv.ID)
				}
				records[osv.ID] = hash
			case "finding":
				var f finding
				if err := json.Unmarshal(raw, &f); err != nil || f.OSV == "" || len(f.Trace) == 0 {
					return fmt.Errorf("invalid finding")
				}
				for _, entry := range f.Trace {
					if entry.Module == "" || (entry.Function != "" && entry.Package == "") {
						return fmt.Errorf("invalid finding frame")
					}
				}
				findings = append(findings, f)
			case "progress":
			case "SBOM":
				sbomCount++
				if sbomCount != 1 {
					return fmt.Errorf("duplicate SBOM")
				}
			default:
				return fmt.Errorf("unknown scanner record %q", key)
			}
		}
	}
	if !configured || sbomCount != 1 {
		return fmt.Errorf("scanner configuration is missing")
	}
	corrected := map[string]bool{}
	for _, f := range findings {
		if _, exists := records[f.OSV]; !exists {
			return fmt.Errorf("finding has no OSV record: %s", f.OSV)
		}
		// Module/package-only entries are informational at the existing symbol level.
		if f.Trace[0].Function == "" {
			continue
		}
		expected, known := correctedRecords[f.OSV]
		if !known || records[f.OSV] != expected || f.Trace[0].Module != echoPath || f.Trace[0].Version != echoVersion {
			return fmt.Errorf("blocking vulnerability: %s", f.OSV)
		}
		if module == nil || module.Path != echoPath || module.Version != echoVersion || module.Sum != echoSum || module.Replace != nil {
			return fmt.Errorf("unverified Echo module identity")
		}
		corrected[f.OSV] = true
	}
	for id := range corrected {
		fmt.Fprintf(os.Stderr, "%s: verified upstream-fixed Echo %s; exact stale Go DB record corrected\n", id, echoVersion)
	}
	return nil
}

func canonical(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// Reject duplicate keys before normalization so they cannot conceal changed
// advisory metadata or scanner configuration.
func readValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("invalid object key")
			}
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("duplicate JSON key %q", name)
			}
			value, err := readValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return nil, fmt.Errorf("unterminated object")
		}
		return object, nil
	case '[':
		array := []any{}
		for decoder.More() {
			value, err := readValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return nil, fmt.Errorf("unterminated array")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter")
	}
}

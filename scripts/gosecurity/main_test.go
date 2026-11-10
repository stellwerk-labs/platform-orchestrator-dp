package main

import (
	"bytes"
	"os"
	"runtime/debug"
	"testing"
)

func TestReviewedEchoCorrection(t *testing.T) {
	report, err := os.ReadFile("testdata/fixed-echo.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	module := &debug.Module{Path: echoPath, Version: echoVersion, Sum: echoSum}
	if err := evaluate(report, "binary", module); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"different version":     bytes.ReplaceAll(report, []byte(echoVersion), []byte("v4.15.3")),
		"changed advisory":      bytes.ReplaceAll(report, []byte("2026-11-05T22:54:32Z"), []byte("2026-11-06T22:54:32Z")),
		"unknown vulnerability": bytes.ReplaceAll(report, []byte("GO-2026-6872"), []byte("GO-2099-9999")),
		"protocol":              bytes.ReplaceAll(report, []byte("v1.0.0"), []byte("v2.0.0")),
		"truncated":             report[:len(report)-4],
		"duplicate key":         bytes.Replace(report, []byte(`"protocol_version":"v1.0.0"`), []byte(`"protocol_version":"bad","protocol_version":"v1.0.0"`), 1),
		"unknown record":        append(bytes.Clone(report), []byte(`{"future":{}}`)...),
		"null SBOM":             bytes.Replace(report, []byte(`"SBOM":{}`), []byte(`"SBOM":null`), 1),
		"configuration late":    append([]byte(`{"progress":{"message":"wrong order"}}`+"\n"), report...),
		"empty":                 nil,
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if err := evaluate(input, "binary", module); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	for name, invalid := range map[string]*debug.Module{
		"absent":      nil,
		"wrong sum":   {Path: echoPath, Version: echoVersion, Sum: "h1:wrong"},
		"replacement": {Path: echoPath, Version: echoVersion, Sum: echoSum, Replace: &debug.Module{Path: "local"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := evaluate(report, "binary", invalid); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestSymbolLevelMatchesGovulncheck(t *testing.T) {
	report, err := os.ReadFile("testdata/fixed-echo.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	// Known-ID corrections never cover an additional called vulnerability.
	additional := []byte("\n" + `{"osv":{"id":"GO-2099-9999"}}` + "\n" + `{"finding":{"osv":"GO-2099-9999","trace":[{"module":"example.com/module","version":"v1.0.0","package":"example.com/module","function":"Called"}]}}`)
	module := &debug.Module{Path: echoPath, Version: echoVersion, Sum: echoSum}
	if err := evaluate(append(bytes.Clone(report), additional...), "binary", module); err == nil {
		t.Fatal("called finding must block")
	}
	informational := bytes.ReplaceAll(additional, []byte(`,"package":"example.com/module","function":"Called"`), nil)
	if err := evaluate(append(bytes.Clone(report), informational...), "binary", module); err != nil {
		t.Fatalf("module-only findings are informational at symbol level: %v", err)
	}
	// Once the official database stops flagging Echo, no correction is needed.
	clean := []byte(`{"config":{"protocol_version":"v1.0.0","scanner_name":"govulncheck","scanner_version":"v1.9.0","scan_level":"symbol","scan_mode":"source","db":"https://vuln.go.dev"}}` + "\n" + `{"SBOM":{}}`)
	if err := evaluate(clean, "source", nil); err != nil {
		t.Fatal(err)
	}
}

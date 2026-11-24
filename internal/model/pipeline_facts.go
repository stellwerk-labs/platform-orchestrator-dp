package model

import "time"

// Pipeline facts are a value-free projection. Logs, tool parameters and private
// captured requests remain behind their existing authorization boundaries.
type PipelineChanges struct {
	Add    int `json:"add"`
	Change int `json:"change"`
	Remove int `json:"remove"`
}
type PipelineStepOutcome struct {
	Verdict     string `json:"verdict"`
	Meaning     string `json:"meaning,omitempty"`
	ToolID      string `json:"tool_id,omitempty"`
	ToolName    string `json:"tool_name,omitempty"`
	ToolVersion string `json:"tool_version,omitempty"`
}
type PipelineStepResult struct {
	ID      string               `json:"id"`
	Phase   string               `json:"phase"`
	Status  string               `json:"status"`
	Code    string               `json:"code,omitempty"`
	Outcome *PipelineStepOutcome `json:"outcome,omitempty"`
}
type PipelineApprovalDecision struct {
	Source         string    `json:"source"`
	Decision       string    `json:"decision"`
	ActorID        string    `json:"actor_id,omitempty"`
	PolicyID       string    `json:"policy_id,omitempty"`
	PolicyVersion  string    `json:"policy_version,omitempty"`
	DecidedAt      time.Time `json:"decided_at"`
	Reason         string    `json:"reason,omitempty"`
	Subject        string    `json:"subject"`
	EvidenceDigest string    `json:"evidence_digest,omitempty"`
}
type PipelineCostCoverage struct {
	Resources    int `json:"resources"`
	Priced       int `json:"priced"`
	Free         int `json:"free"`
	Unknown      int `json:"unknown"`
	Unpriced     int `json:"unpriced"`
	MissingUsage int `json:"missing_usage"`
}
type PipelineCostComparison struct {
	Version        string               `json:"version"`
	Estimator      string               `json:"estimator"`
	Build          string               `json:"build"`
	Currency       string               `json:"currency"`
	Status         string               `json:"status"`
	BeforeMonthly  string               `json:"before_monthly,omitempty"`
	AfterMonthly   string               `json:"after_monthly,omitempty"`
	DeltaMonthly   string               `json:"delta_monthly,omitempty"`
	BeforeCoverage PipelineCostCoverage `json:"before_coverage"`
	AfterCoverage  PipelineCostCoverage `json:"after_coverage"`
	Reason         string               `json:"reason,omitempty"`
	GeneratedAt    time.Time            `json:"generated_at"`
}

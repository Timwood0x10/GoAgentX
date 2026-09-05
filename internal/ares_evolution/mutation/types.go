// Package mutation provides strategy mutation engine for Dream Mode evolution.
// It generates mutated child strategies from parent strategies by varying
// parameters, prompt templates, or tool configurations.
package mutation

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MutationType represents the type of strategy mutation applied.
type MutationType int

const (
	// MutationParameter indicates a parameter value mutation (e.g., temperature change).
	MutationParameter MutationType = iota + 1

	// MutationPrompt indicates a prompt template mutation.
	MutationPrompt

	// MutationTool indicates a tool configuration mutation.
	// The tool configuration (stored in Params["tools"]) is replaced with
	// a different configuration from the tool pool.
	MutationTool

	// MutationCrossover indicates a strategy created via crossover (genetic algorithm).
	// Two parent strategies are combined to produce a child strategy.
	MutationCrossover

	// MutationRoot indicates a root/initial strategy that was not created via
	// mutation or crossover. It represents the baseline strategy from which
	// evolution begins.
	MutationRoot
)

// String returns the human-readable name of the mutation type.
func (mt MutationType) String() string {
	switch mt {
	case MutationParameter:
		return "parameter"
	case MutationPrompt:
		return "prompt"
	case MutationTool:
		return "tool"
	case MutationCrossover:
		return "crossover"
	case MutationRoot:
		return "root"
	default:
		return "unknown"
	}
}

// ParseMutationType converts a string to a MutationType.
// Empty strings are treated as root (default for initial strategies).
// Unknown non-empty strings are logged as a warning and return MutationRoot
// as a safe default, avoiding silent degradation.
func ParseMutationType(s string) MutationType {
	switch s {
	case "parameter":
		return MutationParameter
	case "prompt":
		return MutationPrompt
	case "tool":
		return MutationTool
	case "crossover":
		return MutationCrossover
	case "root", "":
		// Empty string is treated as root (default for initial strategies).
		return MutationRoot
	default:
		log.Warn("unknown mutation type string, falling back to MutationRoot",
			"input", s,
		)
		return MutationRoot
	}
}

// Strategy represents an agent's execution strategy configuration.
type Strategy struct {
	// ID is the unique strategy identifier.
	ID string `json:"id"`

	// ParentID is the parent strategy ID (empty for root strategies).
	ParentID string `json:"parent_id,omitempty"`

	// EvidenceKey is a stable key derived from behaviorally relevant fields
	// (prompt template + normalized numeric params). It enables evidence
	// lookup by phenotype across different strategy IDs.
	EvidenceKey string `json:"evidence_key,omitempty"`

	// Version is the monotonically increasing version number.
	Version int `json:"version"`

	// Name is the human-readable name of the strategy.
	Name string `json:"name,omitempty"`

	// Params holds mutable parameters (temperature, top_k, etc.).
	Params map[string]any `json:"params,omitempty"`

	// PromptTemplate is the behavior prompt template.
	PromptTemplate string `json:"prompt_template,omitempty"`

	// StrategyMutationType records how this strategy was created.
	StrategyMutationType MutationType `json:"strategy_mutation_type"`

	// MutationDesc is a human-readable description of the mutation.
	MutationDesc string `json:"mutation_desc,omitempty"`

	// Score is the current evaluation score (-1 = unevaluated).
	// In single-objective mode, this holds the aggregate score.
	// In multi-objective mode, DimensionScores holds per-dimension values
	// and Score is AggregateDimensions(DimensionScores, weights).
	// This is the canonical fitness — never modified by temporary adjustments.
	Score float64 `json:"score"`

	// SelectionScore holds the fitness-adjusted score used for parent selection.
	// Unlike Score (canonical fitness), this may be modified by fitness sharing
	// or other diversity-preserving mechanisms. Defaults to Score when not set.
	// Consumers should use SelectionScore for selection decisions and Score
	// for reporting/history.
	SelectionScore float64 `json:"selection_score,omitempty"`

	// DimensionScores holds per-objective scores for multi-objective evaluation.
	// Keys are dimension names (e.g. "success_rate", "cost", "latency", "quality").
	// Nil means single-objective mode (backward compatible).
	// When non-nil, Score should be the aggregate of these values.
	DimensionScores map[string]float64 `json:"dimension_scores,omitempty"`

	// CreatedAt is the timestamp when this strategy was created.
	CreatedAt time.Time `json:"created_at"`

	// GenerationCreated is the generation number when this strategy first entered
	// the population. Used by AgentMaxAge eviction: if currentGen - GenerationCreated
	// > AgentMaxAge, the agent is eligible for eviction.
	// 0 = unknown/legacy (never evicted by age — backward-compatible default for
	// strategies created before this field existed).
	GenerationCreated int `json:"generation_created,omitempty"`

	// hashCache caches the StrategyHash result. Set hashValid=false on mutation.
	hashCache  uint64
	hashCached bool
}

// Clone returns a deep copy of the strategy.
// Both Params map and nested slices are copied to avoid shared state.
func (s *Strategy) Clone() *Strategy {
	if s == nil {
		return nil
	}

	clone := &Strategy{
		ID:                   s.ID,
		ParentID:             s.ParentID,
		EvidenceKey:          s.EvidenceKey,
		Version:              s.Version,
		Name:                 s.Name,
		Params:               CloneParams(s.Params),
		PromptTemplate:       s.PromptTemplate,
		StrategyMutationType: s.StrategyMutationType,
		MutationDesc:         s.MutationDesc,
		Score:                s.Score,
		CreatedAt:            s.CreatedAt,
		GenerationCreated:    s.GenerationCreated,
	}
	// hashCache/hashCached are deliberately NOT copied. Clone() is the entry
	// point of every mutation path (Mutator mutates the clone's Params or
	// PromptTemplate afterwards), so an inherited hash would make the child
	// hash-identical to its parent, hit the parent's ScoreCache entry, and
	// return the parent's score as the child's fitness — silently zeroing
	// selection pressure. Leaving the cache cold costs one fnv pass and is
	// fail-safe: a stale hash is unrecoverable, a missing one is just slower.
	if s.DimensionScores != nil {
		clone.DimensionScores = make(map[string]float64, len(s.DimensionScores))
		for k, v := range s.DimensionScores {
			clone.DimensionScores[k] = v
		}
	}
	return clone
}

// HashCached returns true if the StrategyHash has been cached on this object.
func (s *Strategy) HashCached() bool { return s != nil && s.hashCached }

// HashValue returns the cached hash value. Only valid if HashCached() == true.
func (s *Strategy) HashValue() uint64 { return s.hashCache }

// SetHash caches the given hash value on this strategy object.
func (s *Strategy) SetHash(h uint64) {
	if s == nil {
		return
	}
	s.hashCache = h
	s.hashCached = true
}

// ComputeEvidenceKey derives a stable evidence key from behaviorally relevant
// fields: prompt template, sorted numeric params, the tool whitelist, and the
// C5 tool-step attributes (budget/prior).
// The key format is:
// "promptTemplate|key1=value1,key2=value2|tools=t1,t2|budget=N|prior=hint".
// Only numeric values in Params are included, sorted by key for determinism.
// The tool whitelist (Params["tools"]) is normalized (sorted, trimmed) and
// included so two strategies that differ only in tool selection land on
// different EvidenceKeys — otherwise their tool_call evidence would be merged
// and the evolution verdict could not distinguish them by tool choice
// (Y.3-ACT归因入key). The same rule extends to budget and prior (Y1 C5): both
// change execution behavior, so both must change the key, or two strategies
// differing only in budget/prior would share one evidence stream and
// evolution could not select between them.
func (s *Strategy) ComputeEvidenceKey() string {
	if s == nil {
		return ""
	}

	prompt := s.PromptTemplate
	if prompt == "" {
		prompt = "default"
	}

	var pairs []string
	keys := make([]string, 0, len(s.Params))
	for k := range s.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if k == "budget" {
			// Canonicalized separately below: the generic float formatting
			// would record int/float forms ("budget=3.00") but drop the
			// string form ("3") the executor accepts, splitting one
			// phenotype across keys by spelling — or worse, merging a
			// string-budget strategy with a no-budget one.
			continue
		}
		v, ok := numericParam(s.Params[k])
		if !ok {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%.2f", k, v))
	}

	evidenceKey := prompt
	if len(pairs) > 0 {
		evidenceKey = prompt + "|" + strings.Join(pairs, ",")
	}

	// Y.3-ACT: include the tool whitelist in the evidence key so strategies
	// that differ only in tool selection are distinguishable. The value is
	// normalized (split, trim, sort, rejoin) so "b,a" and "a, b" produce the
	// same key — the set is what matters, not the order.
	if tools, ok := s.Params["tools"].(string); ok {
		normalized := normalizeToolKey(tools)
		if normalized != "" {
			evidenceKey = evidenceKey + "|tools=" + normalized
		}
	}

	// Y1 C5: include the normalized per-tool call budget so strategies that
	// differ only in budget are distinguishable. Unlimited (missing /
	// malformed / non-positive) adds no suffix, keeping the pre-budget key
	// stable for the non-evolved path (zero-value usable).
	if budget, ok := normalizeBudgetKey(s.Params); ok {
		evidenceKey += fmt.Sprintf("|budget=%d", budget)
	}

	// Y1 C5: include the trimmed prior hint so strategies that differ only in
	// prior do not merge their evidence. prior is advisory prompt text, but it
	// changes the rendered prompt and hence behavior — without this suffix two
	// priors would share one evidence stream. Empty/missing prior adds no
	// suffix, keeping the pre-prior key stable.
	if prior, ok := s.Params["prior"].(string); ok {
		if trimmed := strings.TrimSpace(prior); trimmed != "" {
			evidenceKey = evidenceKey + "|prior=" + trimmed
		}
	}

	s.EvidenceKey = evidenceKey
	return evidenceKey
}

// normalizeBudgetKey canonicalizes Params["budget"] to a positive int cap.
// It mirrors agents.ToolBudgetFromParams (the execution-side twin, same
// accepted shapes: int, int64, float64, numeric string): every spelling of
// the same cap must map to one key, or one phenotype would split across keys
// by spelling. Returns false for missing/malformed/non-positive values —
// unlimited adds no suffix. The key names ("budget"/"prior") are literals
// matching agents.ParamKeyBudget/ParamKeyPrior; this package stays stdlib-only
// (cf. the pre-existing normalizeToolKey / ToolNamesFromParams split).
func normalizeBudgetKey(params map[string]any) (int, bool) {
	if len(params) == 0 {
		return 0, false
	}
	v, ok := params["budget"]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		if n > 0 {
			return n, true
		}
	case int64:
		if n > 0 {
			return int(n), true
		}
	case float64:
		if n >= 1 {
			return int(n), true
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(n)); err == nil && parsed > 0 {
			return parsed, true
		}
	}
	return 0, false
}

// numericParam normalizes a param value to float64 for evidence-key purposes.
// Params are populated from DefaultParamRanges (which stores untyped int
// literals for top_k / max_steps / memory_limit) and from JSON round-trips
// (which yield float64), so a bare float64 type assertion would silently drop
// every integer dimension and let strategies differing only in top_k collapse
// onto the same EvidenceKey — merging their evidence. Returns false for
// non-numeric values (strings, bools, slices), which stay out of the key.
func numericParam(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

// normalizeToolKey splits a comma-separated tool string into individual names,
// trims whitespace, sorts them, and rejoins with commas. Empty entries are
// skipped. Returns "" if the input is empty or contains no valid names.
func normalizeToolKey(raw string) string {
	parts := strings.Split(raw, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			names = append(names, p)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// ParamRange defines the allowed range for a mutable parameter.
type ParamRange struct {
	// Name is the parameter name (e.g., "temperature").
	Name string

	// Values contains candidate values for this parameter.
	Values []any
}

// DefaultParamRanges provides sensible default parameter ranges for LLM agents.
var DefaultParamRanges = map[string]ParamRange{
	"temperature":        {Name: "temperature", Values: []any{0.1, 0.3, 0.5, 0.7, 0.9}},
	"top_k":              {Name: "top_k", Values: []any{10, 20, 40, 80}},
	"max_steps":          {Name: "max_steps", Values: []any{5, 10, 15, 20}},
	"memory_limit":       {Name: "memory_limit", Values: []any{3, 5, 10}},
	"conflict_threshold": {Name: "conflict_threshold", Values: []any{0.85, 0.90, 0.95}},
}

// CloneParams creates a shallow copy of a params map to avoid shared state.
func CloneParams(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = cloneValue(v)
	}
	return dst
}

// cloneValue performs a shallow-to-moderate copy of a value.
// For slices, it creates a new slice with copied elements.
// For other types, it returns the value as-is (safe for primitives and strings).
func cloneValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []any:
		copied := make([]any, len(val))
		copy(copied, val)
		return copied
	case []string:
		copied := make([]string, len(val))
		copy(copied, val)
		return copied
	case []int:
		copied := make([]int, len(val))
		copy(copied, val)
		return copied
	case []int64:
		copied := make([]int64, len(val))
		copy(copied, val)
		return copied
	case []float64:
		copied := make([]float64, len(val))
		copy(copied, val)
		return copied
	case []bool:
		copied := make([]bool, len(val))
		copy(copied, val)
		return copied
	case map[string]any:
		copied := make(map[string]any, len(val))
		for k, vv := range val {
			copied[k] = cloneValue(vv)
		}
		return copied
	default:
		return v
	}
}

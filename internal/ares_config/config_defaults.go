// Package ares_config provides configuration loading and validation for ares.
// This file contains the default value initialization logic for the Config type.
package ares_config

import (
	"time"
)

// Default string constants used across config defaults. Declared as named
// constants (rather than inline literals) so goconst stays quiet and the
// values are grep-able.
const (
	defaultServerHost   = "localhost"
	defaultLLMProvider  = "ollama"
	defaultLLMModel     = "gemma4"
	defaultOutputFormat = "simple"
	defaultStorageType  = "postgres"
	defaultPGVectorTbl  = "embeddings"
	providerOpenAI      = "openai"
	providerOpenRouter  = "openrouter"
	providerAnthropic   = "anthropic"
)

// DefaultArchiveDir is the default round-archive directory. Exported so the
// minimal service path (api_impl) can reuse the exact same default without
// duplicating the literal, keeping the two wiring paths in sync.
const DefaultArchiveDir = ".context/rounds"

// DefaultEvolution* are the config-layer defaults setDefaults applies when the
// YAML leaves an evolution field unset. Exported so the bootstrap GA wiring can
// tell "operator tuned this field" apart from "setDefaults filled it in": every
// field is non-zero by the time Bootstrap runs, so a plain non-zero guard
// cannot make that distinction (see applyGATuning).
//
// These deliberately differ from the GA engine's own defaults in
// ares_evolution.DefaultSystemConfig (e.g. EliteCount 2 vs 3, BreedingPoolRatio
// 0.5 vs 0.6); fields the operator did not tune must keep the engine values.
const (
	DefaultEvolutionPopulationSize    = 20
	DefaultEvolutionEliteCount        = 2
	DefaultEvolutionSurvivalRate      = 0.6
	DefaultEvolutionMutationRate      = 0.2
	DefaultEvolutionMinMutationRate   = 0.05
	DefaultEvolutionMaxMutationRate   = 0.5
	DefaultEvolutionGenerations       = 15
	DefaultEvolutionBreedingPoolRatio = 0.5
	DefaultEvolutionSelectionStrategy = "tournament"
)

// M4-D0: DefaultToolProjection* removed with their package.

// NewMinimalConfig builds a fully-runnable Config from only the LLM endpoint
// details, so a user does not need a YAML file to start the runtime: everything
// else (agents, memory, tools, storage, kernel policy) falls back to the
// package defaults via setDefaults.
//
// Provider is inferred: a non-empty apiKey selects the OpenAI-compatible
// provider (works for any OpenAI-compatible endpoint); otherwise ollama.
// Memory is force-enabled because the kernel scheduler contract requires a
// MemoryManager for checkpoint/context wiring (see validateServeConfig) —
// enabling it here is the only non-default choice the minimal path must make.
//
// Args:
//   - baseURL: the LLM endpoint root, e.g. "https://api.openai.com/v1" or
//     "http://localhost:11434/v1". Empty falls back to the provider default.
//   - apiKey: the API key (empty for local ollama).
//   - model: the model name. Empty selects the provider default.
//
// Returns:
//   - *Config: a fully-defaulted, validated config ready for Bootstrap.
func NewMinimalConfig(baseURL, apiKey, model string) *Config {
	cfg := &Config{}
	cfg.LLM.BaseURL = baseURL
	cfg.LLM.APIKey = apiKey
	cfg.LLM.Provider = providerOpenAI
	if apiKey == "" {
		cfg.LLM.Provider = defaultLLMProvider // ollama
	}
	cfg.LLM.Model = model
	// Memory defaults to enabled (nil Enabled field → IsEnabled() == true), so
	// a minimal startup always satisfies the kernel scheduler's Memory
	// requirement.
	cfg.setDefaults()
	if cfg.LLM.Model == "" {
		if cfg.LLM.Provider == providerOpenAI {
			cfg.LLM.Model = "gpt-4o-mini"
		} else {
			cfg.LLM.Model = defaultLLMModel
		}
	}
	// Assemble a default agent population so the runtime is immediately
	// capable of task division (coder / reviewer / researcher), even with no
	// config file. A user who wants different agents supplies a config file
	// instead.
	cfg.Agents.Sub = defaultSubAgents()
	return cfg
}

// defaultSubAgents returns the standard capability team wired by the minimal
// config path. Types mirror the demo/monitor-live fleet: an analysis/coder, a
// recommendation/reviewer and a research agent, each with the triggers that
// route profile fields to them.
func defaultSubAgents() []SubAgentConfig {
	return []SubAgentConfig{
		{
			ID:       "coder-a",
			Type:     "coder",
			Category: "analysis",
			Triggers: []string{"analysis", "code"},
		},
		{
			ID:       "reviewer-1",
			Type:     "reviewer",
			Category: "recommendation",
			Triggers: []string{"recommendation", "optimization"},
		},
		{
			ID:       "researcher-1",
			Type:     "researcher",
			Category: "research",
			Triggers: []string{"research", "knowledge"},
		},
	}
}

//nolint:gocyclo // Complex default value initialization for multiple config sections
func (c *Config) setDefaults() {
	if c.Server.Host == "" {
		c.Server.Host = defaultServerHost
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.LLM.Provider == "" {
		c.LLM.Provider = defaultLLMProvider
	}
	if c.LLM.Model == "" {
		c.LLM.Model = defaultLLMModel
	}
	if c.LLM.Timeout == 0 {
		c.LLM.Timeout = 60
	}
	if c.LLM.MaxTokens == 0 {
		c.LLM.MaxTokens = 4096
	}
	if c.LLM.ScorerAPIRate == 0 {
		c.LLM.ScorerAPIRate = 10
	}
	if c.LLM.ScorerAPIBurst == 0 {
		c.LLM.ScorerAPIBurst = 20
	}
	if c.Output.Format == "" {
		c.Output.Format = defaultOutputFormat
	}
	if c.Output.ItemTemplate == "" {
		c.Output.ItemTemplate = "{{.ItemID}}: {{.Name}} ({{.Price}})"
	}
	if c.Output.SummaryTemplate == "" {
		c.Output.SummaryTemplate = "Got {{.Count}} recommendations"
	}
	// Prompt templates default so a config that omits the prompts section
	// still renders a meaningful worker prompt. Before this, an empty
	// prompts.recommendation rendered an empty prompt and every worker LLM
	// call failed with a provider 400 (empty user content), burning the
	// failover cooldown (20s) per call.
	if c.Prompts.Recommendation == "" {
		c.Prompts.Recommendation = DefaultRecommendationPrompt
	}
	if c.Prompts.ProfileExtraction == "" {
		c.Prompts.ProfileExtraction = DefaultProfileExtractionPrompt
	}
	if c.Prompts.StyleAnalysis == "" {
		c.Prompts.StyleAnalysis = DefaultStyleAnalysisPrompt
	}
	// Storage defaults
	if c.Storage.Type == "" {
		c.Storage.Type = defaultStorageType
	}
	if c.Storage.Port == 0 {
		c.Storage.Port = 5432
	}
	if c.Storage.PGVector.Dimension == 0 {
		c.Storage.PGVector.Dimension = 1536
	}
	if c.Storage.PGVector.TableName == "" {
		c.Storage.PGVector.TableName = defaultPGVectorTbl
	}
	// Memory defaults
	if c.Memory.SessionMemory.MaxHistory == 0 {
		c.Memory.SessionMemory.MaxHistory = 50
	}
	if c.Memory.UserProfile.Storage == "" {
		c.Memory.UserProfile.Storage = "memory"
	}
	if c.Memory.TaskDistillation.Prompt == "" {
		c.Memory.TaskDistillation.Prompt = DefaultTaskDistillationPrompt
	}
	// Closed-loop memory defaults. MaxHistory defaults to 10 when zero — this
	// is the closed-loop context window, distinct from SessionMemory.MaxHistory.
	if c.Memory.MaxHistory == 0 {
		c.Memory.MaxHistory = 10
	}
	// Distillation defaults (P0-3 decision: default TRUE). EnableDistillation
	// is a *bool: nil (unset) → true, so deployments relying on
	// Storage+Embedding alone keep distillation after the C1 gate landed — an
	// explicit `false` in YAML is the only way to disable it.
	if c.Memory.EnableDistillation == nil {
		t := true
		c.Memory.EnableDistillation = &t
	}
	if c.Memory.DistillationEnabled() && c.Memory.DistillationThreshold == 0 {
		c.Memory.DistillationThreshold = 3
	}
	// RAG defaults: only apply TopK/MinScore defaults when RAG is opted in.
	// When EnableRAG is false, leave them at zero so retrieval stays inert.
	if c.Memory.EnableRAG {
		if c.Memory.RAGTopK == 0 {
			c.Memory.RAGTopK = 5
		}
		if c.Memory.RAGMinScore == 0 {
			c.Memory.RAGMinScore = 0.4
		}
	}
	// Archive defaults: dir and max_rounds apply regardless so the values are
	// always valid; Enabled is *bool so its default-on semantics need no setting here.
	if c.Memory.Archive.Dir == "" {
		c.Memory.Archive.Dir = DefaultArchiveDir
	}
	if c.Memory.Archive.MaxRounds == 0 {
		c.Memory.Archive.MaxRounds = 200
	}
	// Knowledge (AKG) defaults: only apply TopK/MinScore defaults when
	// retrieval is opted in. When RetrievalEnabled is false, leave them at
	// zero so AKG retrieval stays inert.
	if c.Knowledge.RetrievalEnabled {
		if c.Knowledge.TopK == 0 {
			c.Knowledge.TopK = 5
		}
		if c.Knowledge.MinScore == 0 {
			c.Knowledge.MinScore = 0.4
		}
	}
	// Validation defaults
	if c.Validation.SchemaType == "" {
		c.Validation.SchemaType = "default" // "default", "travel", "custom"
	}
	if c.Validation.MaxRetries == 0 {
		c.Validation.MaxRetries = 3
	}
	// Workflow defaults
	if c.Workflow.ReloadInterval == 0 && c.Workflow.AutoReload {
		c.Workflow.ReloadInterval = 30 // seconds
	}
	// MCP defaults
	for i := range c.MCP.Servers {
		if c.MCP.Servers[i].Timeout == 0 {
			c.MCP.Servers[i].Timeout = 30
		}
	}
	// Evolution defaults
	if c.Evolution.PopulationSize == 0 {
		c.Evolution.PopulationSize = DefaultEvolutionPopulationSize
	}
	if c.Evolution.EliteCount == 0 {
		c.Evolution.EliteCount = DefaultEvolutionEliteCount
	}
	if c.Evolution.SurvivalRate == 0 {
		c.Evolution.SurvivalRate = DefaultEvolutionSurvivalRate
	}
	if c.Evolution.MutationRate == 0 {
		c.Evolution.MutationRate = DefaultEvolutionMutationRate
	}
	if c.Evolution.MinMutationRate == 0 {
		c.Evolution.MinMutationRate = DefaultEvolutionMinMutationRate
	}
	if c.Evolution.MaxMutationRate == 0 {
		c.Evolution.MaxMutationRate = DefaultEvolutionMaxMutationRate
	}
	if c.Evolution.Generations == 0 {
		c.Evolution.Generations = DefaultEvolutionGenerations
	}
	if c.Evolution.BreedingPoolRatio == 0 {
		c.Evolution.BreedingPoolRatio = DefaultEvolutionBreedingPoolRatio
	}
	if c.Evolution.MinInterval == "" {
		c.Evolution.MinInterval = "5m"
	}
	if c.Evolution.SelectionStrategy == "" {
		c.Evolution.SelectionStrategy = DefaultEvolutionSelectionStrategy
	}
	if c.Evolution.TournamentSize == 0 {
		c.Evolution.TournamentSize = 3
	}
	if c.Evolution.CrossoverType == "" {
		c.Evolution.CrossoverType = "uniform"
	}
	// LLM scoring defaults — MaxCallsPerGeneration caps LLM API cost per
	// generation. When zero, use 100 (matches the tiered scorer default).
	if c.Evolution.LLMScoring.MaxCallsPerGeneration == 0 {
		c.Evolution.LLMScoring.MaxCallsPerGeneration = 100
	}
	// M4-D0: tool_projection defaults removed with their package.
	// Discovery defaults — opt-in via Enabled (default false). When enabled
	// but Interval is unset, default to 5 minutes between discovery cycles.
	if c.Discovery.Interval == 0 {
		c.Discovery.Interval = 5 * time.Minute
	}
}

// Package ares_config provides configuration loading and validation for ares.
// This file contains the configuration validation logic for the Config type.
package ares_config

import (
	"errors"
	"fmt"
)

// Validate validates the configuration values.
func (c *Config) Validate() error {
	if err := c.validateServer(); err != nil {
		return err
	}

	if err := c.validateLLM(); err != nil {
		return err
	}

	if err := c.validateAgents(); err != nil {
		return err
	}

	if err := c.validateOutput(); err != nil {
		return err
	}

	if err := c.validateStorage(); err != nil {
		return err
	}

	if err := c.validateMemory(); err != nil {
		return err
	}

	if err := c.validateKnowledge(); err != nil {
		return err
	}

	if err := c.validateMCP(); err != nil {
		return err
	}

	if err := c.validateEvolution(); err != nil {
		return err
	}

	if err := c.validateDiscovery(); err != nil {
		return err
	}

	if err := c.validateKernel(); err != nil {
		return err
	}

	return nil
}

// validateServer validates server configuration
func (c *Config) validateServer() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d, must be between 1 and 65535", c.Server.Port)
	}
	return nil
}

// validateLLM validates LLM configuration
func (c *Config) validateLLM() error {
	if c.LLM.Timeout < 1 {
		return fmt.Errorf("invalid LLM timeout: %d, must be positive", c.LLM.Timeout)
	}
	if c.LLM.MaxTokens < 1 {
		return fmt.Errorf("invalid LLM max tokens: %d, must be positive", c.LLM.MaxTokens)
	}
	validProviders := map[string]bool{
		providerOpenAI:     true,
		defaultLLMProvider: true,
		providerOpenRouter: true,
		providerAnthropic:  true,
	}
	if !validProviders[c.LLM.Provider] {
		return fmt.Errorf("invalid LLM provider: %s, must be 'openai', 'ollama', 'openrouter', or 'anthropic'", c.LLM.Provider)
	}
	return nil
}

// validateAgents validates agents configuration
func (c *Config) validateAgents() error {
	for i, subAgent := range c.Agents.Sub {
		if err := c.validateSubAgent(i, subAgent); err != nil {
			return err
		}
	}
	return nil
}

// validateSubAgent validates a single sub-agent configuration
func (c *Config) validateSubAgent(i int, subAgent SubAgentConfig) error {
	if subAgent.ID == "" {
		return fmt.Errorf("sub-agent %d: ID cannot be empty", i)
	}
	if subAgent.Type == "" {
		return fmt.Errorf("sub-agent %d: Type cannot be empty", i)
	}
	if subAgent.Timeout < 1 {
		return fmt.Errorf("sub-agent %d: timeout must be positive", i)
	}
	if subAgent.MaxRetries < 0 {
		return fmt.Errorf("sub-agent %d: max retries must be non-negative", i)
	}
	return nil
}

// validateOutput validates output configuration
func (c *Config) validateOutput() error {
	validFormats := map[string]bool{"table": true, "json": true, defaultOutputFormat: true}
	if !validFormats[c.Output.Format] {
		return fmt.Errorf("invalid output format: %s, must be 'table', 'json', or 'simple'", c.Output.Format)
	}
	if c.Validation.MaxRetries < 0 {
		return fmt.Errorf("invalid validation max retries: %d, must be non-negative", c.Validation.MaxRetries)
	}
	return nil
}

// validateStorage validates storage configuration
func (c *Config) validateStorage() error {
	if !c.Storage.Enabled {
		return nil
	}

	if c.Storage.Host == "" {
		return errors.New("storage enabled but host is empty")
	}
	if c.Storage.Port < 1 || c.Storage.Port > 65535 {
		return fmt.Errorf("invalid storage port: %d, must be between 1 and 65535", c.Storage.Port)
	}
	if c.Storage.Database == "" {
		return errors.New("storage enabled but database name is empty")
	}
	return nil
}

// validateMemory validates memory configuration
func (c *Config) validateMemory() error {
	if c.Memory.SessionMemory.MaxHistory < 0 {
		return fmt.Errorf("invalid session memory max history: %d, must be non-negative", c.Memory.SessionMemory.MaxHistory)
	}
	// Distillation threshold semantics: 0 preserves legacy ungated behaviour
	// (fires on every event), negative is invalid. Positive gates rounds.
	if c.Memory.TaskDistillation.Threshold < 0 {
		return fmt.Errorf("invalid task_distillation threshold: %d, must be non-negative", c.Memory.TaskDistillation.Threshold)
	}
	// Closed-loop MaxHistory is independent of SessionMemory.MaxHistory.
	// Negative is invalid; zero is allowed (default applied in setDefaults).
	if c.Memory.MaxHistory < 0 {
		return fmt.Errorf("invalid memory max_history: %d, must be non-negative", c.Memory.MaxHistory)
	}
	// DistillationThreshold is invalid when negative. Zero is allowed —
	// setDefaults only fills it when EnableDistillation is true.
	if c.Memory.DistillationThreshold < 0 {
		return fmt.Errorf("invalid memory distillation_threshold: %d, must be non-negative", c.Memory.DistillationThreshold)
	}
	// RAG validation: only enforce when opted in. When EnableRAG is false,
	// RAGTopK/RAGMinScore may stay zero — defaults are not applied here so
	// retrieval remains inert until the operator explicitly enables it.
	if c.Memory.EnableRAG {
		if c.Memory.RAGTopK < 0 {
			return fmt.Errorf("invalid memory rag_top_k: %d, must be non-negative", c.Memory.RAGTopK)
		}
		if c.Memory.RAGMinScore < 0 {
			return fmt.Errorf("invalid memory rag_min_score: %f, must be non-negative", c.Memory.RAGMinScore)
		}
	}
	// Archive validation: only enforce when active. Defaults guarantee Dir and
	// MaxRounds are set, but validate defensively in case defaults were skipped.
	if c.Memory.Archive.IsEnabled() {
		if c.Memory.Archive.Dir == "" {
			return errors.New("archive dir must be non-empty when archive is enabled")
		}
		if c.Memory.Archive.MaxRounds <= 0 {
			return fmt.Errorf("invalid archive max_rounds: %d, must be positive", c.Memory.Archive.MaxRounds)
		}
	}
	return nil
}

// validateKnowledge validates AKG knowledge retrieval configuration.
// When RetrievalEnabled is false (the default), no validation is performed
// so prior behavior is preserved.
func (c *Config) validateKnowledge() error {
	if !c.Knowledge.RetrievalEnabled {
		return nil
	}
	if c.Knowledge.TopK < 0 {
		return fmt.Errorf("invalid knowledge top_k: %d, must be non-negative", c.Knowledge.TopK)
	}
	if c.Knowledge.MinScore < 0 {
		return fmt.Errorf("invalid knowledge min_score: %f, must be non-negative", c.Knowledge.MinScore)
	}
	return nil
}

// validateMCP validates MCP configuration
func (c *Config) validateMCP() error {
	serverNames := make(map[string]bool)
	for i, srv := range c.MCP.Servers {
		if err := c.validateMCPServer(i, srv, serverNames); err != nil {
			return err
		}
		serverNames[srv.Name] = true
	}
	return nil
}

// validateMCPServer validates a single MCP server configuration
func (c *Config) validateMCPServer(i int, srv MCPServerEntry, serverNames map[string]bool) error {
	if srv.Name == "" {
		return fmt.Errorf("mcp server %d: name must not be empty", i)
	}
	if serverNames[srv.Name] {
		return fmt.Errorf("mcp server %d: duplicate name %q", i, srv.Name)
	}
	if srv.Transport.Type != "stdio" && srv.Transport.Type != "sse" {
		return fmt.Errorf("mcp server %q: transport type must be \"stdio\" or \"sse\", got %q", srv.Name, srv.Transport.Type)
	}

	if err := c.validateMCPTransport(srv); err != nil {
		return err
	}

	if srv.Timeout < 0 {
		return fmt.Errorf("mcp server %q: timeout must be non-negative, got %d", srv.Name, srv.Timeout)
	}
	return nil
}

// validateMCPTransport validates MCP transport configuration
func (c *Config) validateMCPTransport(srv MCPServerEntry) error {
	if srv.Transport.Type == "stdio" {
		if srv.Transport.Stdio == nil {
			return fmt.Errorf("mcp server %q: stdio transport config must not be nil", srv.Name)
		}
		if srv.Transport.Stdio.Command == "" {
			return fmt.Errorf("mcp server %q: stdio command must not be empty", srv.Name)
		}
	}

	if srv.Transport.Type == "sse" {
		if srv.Transport.SSE == nil {
			return fmt.Errorf("mcp server %q: sse transport config must not be nil", srv.Name)
		}
		if srv.Transport.SSE.URL == "" {
			return fmt.Errorf("mcp server %q: sse url must not be empty", srv.Name)
		}
	}
	return nil
}

// validateEvolution validates evolution configuration
func (c *Config) validateEvolution() error {
	if !c.Evolution.Enabled {
		return nil
	}

	if c.Evolution.PopulationSize < 2 {
		return fmt.Errorf("evolution: population_size must be >= 2, got %d", c.Evolution.PopulationSize)
	}
	if c.Evolution.EliteCount < 0 || c.Evolution.EliteCount >= c.Evolution.PopulationSize {
		return fmt.Errorf("evolution: elite_count must be in [0, population_size), got %d", c.Evolution.EliteCount)
	}
	if c.Evolution.SurvivalRate <= 0 || c.Evolution.SurvivalRate > 1 {
		return fmt.Errorf("evolution: survival_rate must be in (0, 1], got %f", c.Evolution.SurvivalRate)
	}
	if c.Evolution.MutationRate < 0 || c.Evolution.MutationRate > 1 {
		return fmt.Errorf("evolution: mutation_rate must be in [0, 1], got %f", c.Evolution.MutationRate)
	}
	if c.Evolution.Generations < 1 {
		return fmt.Errorf("evolution: generations must be >= 1, got %d", c.Evolution.Generations)
	}
	if c.Evolution.LLMScoring.Enabled {
		if c.Evolution.LLMScoring.MaxCallsPerGeneration < 0 {
			return fmt.Errorf("evolution: llm_scoring.max_calls_per_generation must be >= 0, got %d",
				c.Evolution.LLMScoring.MaxCallsPerGeneration)
		}
	}

	return nil
}

// validateDiscovery validates the optional service discovery configuration.
// When discovery is disabled (the default), no validation is performed so the
// discovery packages remain unused and prior behavior is preserved.
func (c *Config) validateDiscovery() error {
	if !c.Discovery.Enabled {
		return nil
	}
	if c.Discovery.Interval < 0 {
		return fmt.Errorf("discovery: interval must be non-negative, got %s", c.Discovery.Interval)
	}
	return nil
}

// validateKernel validates the kernel loop-clock knobs.
//
// Both are zero-value-safe (0 = unlimited rounds / 0 = "every quantum closes a
// round"), so only negatives are rejected. The runtime does normalize negatives
// defensively, but a negative here is always a config mistake — reporting it
// beats silently substituting a default the operator did not ask for.
func (c *Config) validateKernel() error {
	if c.Kernel.LoopMaxIterations < 0 {
		return fmt.Errorf("kernel: loop_max_iterations must be non-negative (0 = unlimited), got %d",
			c.Kernel.LoopMaxIterations)
	}
	if c.Kernel.LoopRoundQuanta < 0 {
		return fmt.Errorf("kernel: loop_round_quanta must be non-negative (0 = default 1), got %d",
			c.Kernel.LoopRoundQuanta)
	}
	if c.Kernel.DAGExecution.MaxPlanDepth < 0 {
		return fmt.Errorf("kernel: dag_execution.max_plan_depth must be non-negative (0 = planner default), got %d",
			c.Kernel.DAGExecution.MaxPlanDepth)
	}
	if c.Kernel.DAGExecution.ReaperGrace < 0 {
		return fmt.Errorf("kernel: dag_execution.reaper_grace must be non-negative (0 = default 30s), got %s",
			c.Kernel.DAGExecution.ReaperGrace)
	}
	return nil
}

// package graph - provides dynamic agent orchestration with pluggable scheduling.

package graph

import (
	"context"
	"fmt"

	"github.com/Timwood0x10/ares/internal/agents/base"
	"github.com/Timwood0x10/ares/internal/errors"
)

// Node represents an executable unit in the graph.
//
// Node implementations are adapted to ares_runtime.Executable by the unified
// Runner's adapter layer: the adapter maps between *State (graph-local) and
// *ExecutionContext (unified scope) at execution time.
type Node interface {
	// Execute runs the node with the given state.
	Execute(ctx context.Context, state *State) error
	// ID returns the unique identifier of the node.
	ID() string
}

// AgentNode wraps an existing agent to be used as a node.
type AgentNode struct {
	agent base.Agent
}

// NewAgentNode creates a new agent node.
//
// Args:
// agent - agent instance, must not be nil.
// Returns new agent node or error.
func NewAgentNode(agent base.Agent) (*AgentNode, error) {
	if agent == nil {
		return nil, errors.New("agent cannot be nil")
	}
	return &AgentNode{agent: agent}, nil
}

// Execute runs the agent node.
func (n *AgentNode) Execute(ctx context.Context, state *State) error {
	if n == nil || n.agent == nil {
		return errors.New("agent node is not initialized")
	}

	input, exists := state.Get("input")
	if !exists || input == nil {
		return fmt.Errorf("agent %s: input not found in state", n.ID())
	}
	result, err := n.agent.Process(ctx, input)
	if err != nil {
		return errors.Wrapf(err, "agent %s execution failed", n.ID())
	}

	state.Set("node."+n.ID(), result)
	return nil
}

// ID returns the agent ID.
func (n *AgentNode) ID() string {
	if n == nil || n.agent == nil {
		return ""
	}
	return n.agent.ID()
}

// FuncNode wraps a simple function to be used as a node.
type FuncNode struct {
	id string
	fn func(context.Context, *State) error
}

// NewFuncNode creates a new function node.
//
// Args:
// id - unique node identifier, must not be empty.
// fn - function to execute, must not be nil.
// Returns new function node or error.
func NewFuncNode(id string, fn func(context.Context, *State) error) (*FuncNode, error) {
	if id == "" {
		return nil, errors.New("node id cannot be empty")
	}
	if fn == nil {
		return nil, errors.New("function cannot be nil")
	}
	return &FuncNode{id: id, fn: fn}, nil
}

// Execute runs the function node.
func (n *FuncNode) Execute(ctx context.Context, state *State) error {
	if n == nil || n.fn == nil {
		return errors.New("function node is not initialized")
	}

	err := n.fn(ctx, state)
	if err != nil {
		return errors.Wrapf(err, "function %s execution failed", n.ID())
	}

	return nil
}

// ID returns the function node ID.
func (n *FuncNode) ID() string {
	if n == nil {
		return ""
	}
	return n.id
}

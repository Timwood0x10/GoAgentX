package sub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// schemaStubBinder is a ToolBinder stub with one canned schema: grep declares
// {limit, query} while calls may pass only {query}.
type schemaStubBinder struct {
	ToolBinder
	schemas []resources.ToolSchema
}

func (b *schemaStubBinder) GetToolSchemas() []resources.ToolSchema { return b.schemas }

// TestObserveToolCalls_AttributesBySchemaShape locks M6: a call that omits an
// optional parameter still attributes to the schema-derived ToolClassID
// (tool#limit,query), not the args-derived shape (tool#query) — otherwise the
// record misses the L1 node and WindowToolStep stays empty.
func TestObserveToolCalls_AttributesBySchemaShape(t *testing.T) {
	inner := NewToolBinder()
	inner.BindTool("grep", func(context.Context, map[string]any) (any, error) {
		return "hit", nil
	})
	stub := &schemaStubBinder{
		ToolBinder: inner,
		schemas: []resources.ToolSchema{{
			Name: "grep",
			Parameters: &resources.ParameterSchema{
				Properties: map[string]*resources.Parameter{
					"query": {},
					"limit": {},
				},
			},
		}},
	}
	obs := &recordingToolObserver{}
	binder := ObserveToolCalls(stub, obs)

	_, err := binder.CallTool(context.Background(), "grep", map[string]any{"query": "x"})
	require.NoError(t, err)
	require.Len(t, obs.got, 1)
	assert.Equal(t, "grep#limit,query", obs.got[0].ToolStepID,
		"attribution must use the declared schema shape, not the call's args")
}

// TestObserveToolCalls_UnknownToolFallsBackToArgsShape pins the fallback: a
// tool with no known schema keeps the legacy args-derived key.
func TestObserveToolCalls_UnknownToolFallsBackToArgsShape(t *testing.T) {
	inner := NewToolBinder()
	inner.BindTool("mystery", func(context.Context, map[string]any) (any, error) {
		return "hit", nil
	})
	stub := &schemaStubBinder{ToolBinder: inner}
	obs := &recordingToolObserver{}
	binder := ObserveToolCalls(stub, obs)

	_, err := binder.CallTool(context.Background(), "mystery", map[string]any{"query": "x"})
	require.NoError(t, err)
	require.Len(t, obs.got, 1)
	assert.Equal(t, "mystery#query", obs.got[0].ToolStepID)
}

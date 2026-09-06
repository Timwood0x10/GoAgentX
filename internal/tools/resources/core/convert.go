package core

import (
	"sort"
	"strings"

	llmcore "github.com/Timwood0x10/ares/api/core"
)

// ToolSchemaToLLMTool converts a ToolSchema from the registry to api/core.Tool
// for passing to the LLM Chat API.
func ToolSchemaToLLMTool(schema ToolSchema) llmcore.Tool {
	return llmcore.Tool{
		Type: "function",
		Function: llmcore.FunctionDefinition{
			Name:        schema.Name,
			Description: schema.Description,
			Parameters:  ParameterSchemaToMap(schema.Parameters),
		},
	}
}

// ParameterSchemaToMap converts *ParameterSchema to map[string]interface{}
// for the JSON Schema format expected by api/core.FunctionDefinition.Parameters.
// Returns nil if the schema is nil.
func ParameterSchemaToMap(schema *ParameterSchema) map[string]interface{} {
	if schema == nil {
		return nil
	}
	result := map[string]interface{}{
		"type": schema.Type,
	}
	if len(schema.Properties) > 0 {
		props := make(map[string]interface{}, len(schema.Properties))
		for name, p := range schema.Properties {
			prop := map[string]interface{}{
				"type": p.Type,
			}
			if p.Description != "" {
				prop["description"] = p.Description
			}
			if p.Default != nil {
				prop["default"] = p.Default
			}
			if len(p.Enum) > 0 {
				prop["enum"] = p.Enum
			}
			if p.Min != nil {
				prop["minimum"] = *p.Min
			}
			if p.Max != nil {
				prop["maximum"] = *p.Max
			}
			props[name] = prop
		}
		result["properties"] = props
	}
	if len(schema.Required) > 0 {
		result["required"] = schema.Required
	}
	return result
}

// ToolArgShape is the normalized argument shape of a tool's declared
// parameters: the sorted set of top-level parameter names joined by ",".
// It is the shape half of the ToolClass identity (toolName + "#" + shape).
//
// It lives here, next to ToolSchema, because BOTH ends of the ToolClass
// identity must agree byte-for-byte: the L1 capability-graph builder that
// writes the node IDs, and the planner that looks nodes up before growing an
// L2 tool node. Deriving the shape from the SCHEMA (not from one call's
// arguments) is what makes the two sides match — a call that omits an
// optional parameter must still resolve to the same ToolClass.
func ToolArgShape(s ToolSchema) string {
	if s.Parameters == nil || len(s.Parameters.Properties) == 0 {
		return ""
	}
	keys := make([]string, 0, len(s.Parameters.Properties))
	for k := range s.Parameters.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// ToolClassID builds the ToolClass node identity from a tool name and its
// declared argument shape: "toolName#shape". Shared by the L1 graph builder
// and the L2 planner so the write and read sides cannot drift.
func ToolClassID(toolName, argShape string) string {
	return toolName + "#" + argShape
}

package taskflow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const ToolName = "task_checkpoint"

// Tool is code-owned control flow, not a user registered business tool. It
// only accesses the fenced task session in the trusted execution context.
type Tool struct{}

func (Tool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: ToolName, Description: Instruction + EvidenceInstruction, InputSchema: &tool.Schema{Type: "object", AdditionalProperties: false, Required: []string{"action"}, Properties: map[string]*tool.Schema{
		"action":           {Type: "string", Enum: []any{"inspect", "define", "start", "complete_analysis", "read_result"}},
		"expected_version": {Type: "integer"}, "step_id": {Type: "string"}, "note": {Type: "string"},
		"offset": {Type: "integer", Description: "UTF-8 byte offset for read_result; initially zero"},
		"steps": {Type: "array", Items: &tool.Schema{Type: "object", AdditionalProperties: false, Required: []string{"id", "title", "criterion", "kind"}, Properties: map[string]*tool.Schema{
			"id": {Type: "string"}, "title": {Type: "string"}, "criterion": {Type: "string"}, "kind": {Type: "string", Enum: []any{"analysis", "tool"}}, "depends_on": {Type: "array", Items: &tool.Schema{Type: "string"}}, "tool_id": {Type: "string"}, "tool_version": {Type: "integer"},
			"check": {Type: "object", AdditionalProperties: false, Properties: map[string]*tool.Schema{"path": {Type: "array", Items: &tool.Schema{Type: "string"}}, "equals_json": {Type: "string", Description: "Expected JSON value at path, e.g. \"created\"; exclusive with non_empty"}, "non_empty": {Type: "boolean"}}},
		}}},
	}}}
}
func (Tool) Call(ctx context.Context, args []byte) (any, error) {
	s := FromContext(ctx)
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if s == nil || !ok || execution.TenantID != s.Scope.TenantID || execution.RequestID != s.Scope.RequestID {
		return nil, runtime.ErrTenantScope
	}
	leave, err := runtime.BeginToolCall(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()
	if len(args) > 64<<10 {
		return nil, runtime.ErrInvalidEnvelope
	}
	var in struct {
		Action  string `json:"action"`
		Version int64  `json:"expected_version"`
		StepID  string `json:"step_id"`
		Note    string `json:"note"`
		Steps   []Step `json:"steps"`
		Offset  int    `json:"offset"`
	}
	d := json.NewDecoder(bytes.NewReader(args))
	d.DisallowUnknownFields()
	if err := d.Decode(&in); err != nil {
		return nil, runtime.ErrInvalidEnvelope
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, runtime.ErrInvalidEnvelope
	}
	switch in.Action {
	case "inspect":
		return s.Recover(ctx)
	case "define":
		return s.Define(ctx, in.Version, in.Steps)
	case "start":
		return s.Start(ctx, in.Version, in.StepID)
	case "complete_analysis":
		return s.CompleteAnalysis(ctx, in.Version, in.StepID, in.Note)
	case "read_result":
		return s.ReadResult(ctx, in.StepID, in.Offset)
	default:
		return nil, runtime.ErrInvalidEnvelope
	}
}

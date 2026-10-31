package codeexec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
)

type receiptExecutor struct {
	calls int
	err   error
}

func (*receiptExecutor) CodeBlockDelimiter() codeexecutor.CodeBlockDelimiter {
	return codeexecutor.CodeBlockDelimiter{Start: "```", End: "```"}
}
func (e *receiptExecutor) ExecuteCode(context.Context, codeexecutor.CodeExecutionInput) (codeexecutor.CodeExecutionResult, error) {
	e.calls++
	return codeexecutor.CodeExecutionResult{}, e.err
}
func TestCodeExecutionCompletionRequiresActualSuccessfulExecution(t *testing.T) {
	for _, input := range []string{`{}`, `{"code_blocks":[]}`, `{"code_blocks":[{"language":"unsupported","code":"x"}]}`, `{"code_blocks":[{"language":"bash","code":"true"}]}`} {
		e := &receiptExecutor{}
		tool := checkedTool{name: "code_exec", executor: e}
		r, err := tool.Call(context.Background(), []byte(input))
		if e.calls == 0 {
			if err == nil {
				t.Fatalf("SDK validation reply accepted: %s => %+v", input, r)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(r)
		var result map[string]any
		_ = json.Unmarshal(b, &result)
		if result["status"] != "completed" || result["output"] != "" {
			t.Fatalf("valid empty stdout was not successful: %s", b)
		}
		if tool.RecoveryPolicy() != servicetool.RecoveryManual || tool.TaskEvidenceCheck() == nil {
			t.Fatal("missing conservative recovery contract")
		}
	}
	e := &receiptExecutor{err: errors.New("script exited nonzero")}
	if _, err := (checkedTool{name: "code_exec", executor: e}).Call(context.Background(), []byte(`{"code_blocks":[{"language":"bash","code":"exit 9"}]}`)); !errors.Is(err, e.err) {
		t.Fatalf("failed script accepted: %v", err)
	}
}

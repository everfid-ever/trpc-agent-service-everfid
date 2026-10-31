package session

import (
	"encoding/json"
	"testing"

	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

func historyEvent(id, request string, fence uint64, m model.Message) agentevent.Event {
	e := agentevent.Event{ID: id, RequestID: request, InvocationID: "invocation", Response: &model.Response{Choices: []model.Choice{{Message: m}}}, Extensions: map[string]json.RawMessage{}}
	e.Extensions[historyExtension], _ = json.Marshal(historyScope{RequestID: request, Fence: fence})
	return e
}
func historyCall(id string) model.Message {
	return model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: id, Type: "function", Function: model.FunctionDefinitionParam{Name: "lookup", Arguments: []byte(`{}`)}}}}
}
func historyResult(id string) model.Message {
	return model.Message{Role: model.RoleTool, ToolID: id, ToolName: "lookup", Content: "receipt"}
}

func TestHistoryViewFiltersRetriesAndOrphansWithoutChangingAuditTrail(t *testing.T) {
	raw := &agentsession.Session{Events: []agentevent.Event{
		historyEvent("previous-old", "previous", 1, model.NewUserMessage("same text")),
		historyEvent("previous-new", "previous", 2, model.NewUserMessage("same text")),
		historyEvent("paired-call", "previous", 2, historyCall("paired")),
		historyEvent("paired-result", "previous", 2, historyResult("paired")),
		historyEvent("orphan-call", "previous", 2, historyCall("missing")),
		historyEvent("orphan-result", "previous", 2, historyResult("absent")),
		historyEvent("late-old", "previous", 1, model.NewAssistantMessage("stale late answer")),
		historyEvent("retry-user", "current", 1, model.NewUserMessage("same text")),
		historyEvent("retry-answer", "current", 1, model.NewAssistantMessage("unfinished")),
		historyEvent("new-user", "current", 2, model.NewUserMessage("same text")),
		historyEvent("live-call", "current", 2, historyCall("live")),
	}, State: agentsession.StateMap{"keep": []byte("state")}}
	raw.Events = append(raw.Events, raw.Events[1])
	before, _ := json.Marshal(raw.Events)
	turn := &BufferedTurn{}
	turn.ConfigureHistory("current", 2, "")
	view := turn.historyView(raw)
	want := []string{"previous-new", "paired-call", "paired-result", "new-user", "live-call"}
	if len(view.Events) != len(want) {
		t.Fatalf("events=%+v", view.Events)
	}
	for i, id := range want {
		if view.Events[i].ID != id {
			t.Fatalf("event %d=%s want %s", i, view.Events[i].ID, id)
		}
	}
	view.Events[0].Choices[0].Message.Content = "mutated read view"
	view.State["keep"][0] = 'X'
	after, _ := json.Marshal(raw.Events)
	if string(before) != string(after) || string(raw.State["keep"]) != "state" {
		t.Fatal("read view mutated persistent data")
	}
}

func TestHistoryConfirmationRetainsOnlyApprovedCallAndOriginalUser(t *testing.T) {
	m := historyCall("approved")
	m.ToolCalls = append(m.ToolCalls, historyCall("unapproved").ToolCalls[0])
	raw := &agentsession.Session{Events: []agentevent.Event{
		historyEvent("input", "current", 1, model.NewUserMessage("create note")),
		historyEvent("ask", "current", 1, m),
		historyEvent("speculation", "current", 1, model.NewAssistantMessage("possibly done")),
		historyEvent("receipt", "current", 2, historyResult("approved")),
	}}
	turn := &BufferedTurn{}
	turn.ConfigureHistory("current", 2, "approved")
	view := turn.historyView(raw)
	if len(view.Events) != 3 || view.Events[1].ID != "ask" || len(view.Events[1].Choices[0].Message.ToolCalls) != 1 || view.Events[1].Choices[0].Message.ToolCalls[0].ID != "approved" {
		t.Fatalf("view=%+v", view.Events)
	}
}

func TestHistoryStampOverridesUntrustedMetadataAndKeepsIDs(t *testing.T) {
	turn := &BufferedTurn{}
	turn.ConfigureHistory("trusted", 7, "")
	e := historyEvent("stable", "untrusted", 999, model.NewUserMessage("hello"))
	v := turn.stampEvent(&e)
	if v.ID != "stable" || eventScope(v).RequestID != "trusted" || eventScope(v).Fence != 7 || eventScope(&e).Fence != 999 {
		t.Fatal("invalid trusted history stamp")
	}
}

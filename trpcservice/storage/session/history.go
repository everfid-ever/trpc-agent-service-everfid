package session

import (
	"encoding/json"
	"strconv"

	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

const historyExtension = "trpc.execution.v1"

type historyScope struct {
	RequestID      string `json:"request_id"`
	Fence          uint64 `json:"fence"`
	PreserveCallID string `json:"-"`
}

// ConfigureHistory binds SDK events to a trusted root request and lease attempt.
// Filtering changes the runner's read view only, never the persisted audit trail.
func (t *BufferedTurn) ConfigureHistory(request string, fence uint64, preserveCall string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.history = historyScope{RequestID: request, Fence: fence, PreserveCallID: preserveCall}
}
func (t *BufferedTurn) historyScope() historyScope {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.history
}
func (t *BufferedTurn) stampEvent(e *agentevent.Event) *agentevent.Event {
	s := t.historyScope()
	if s.RequestID == "" || s.Fence == 0 {
		return e
	}
	v := cloneHistoryEvent(e)
	if v.Extensions == nil {
		v.Extensions = make(map[string]json.RawMessage)
	}
	v.Extensions[historyExtension], _ = json.Marshal(s)
	return v
}
func cloneHistoryEvent(e *agentevent.Event) *agentevent.Event {
	v := e.Clone()
	// SDK Clone assigns a fresh ID; a read view must retain event identity.
	v.ID = e.ID
	return v
}
func eventScope(e *agentevent.Event) historyScope {
	var s historyScope
	if json.Unmarshal(e.Extensions[historyExtension], &s) != nil || s.RequestID == "" || s.Fence == 0 {
		return historyScope{RequestID: e.RequestID}
	}
	return s
}
func (t *BufferedTurn) historyView(raw *agentsession.Session) *agentsession.Session {
	s := t.historyScope()
	if raw == nil || s.RequestID == "" || s.Fence == 0 {
		return raw
	}
	v := raw.Clone()
	latest := map[string]uint64{s.RequestID: s.Fence}
	for i := range v.Events {
		x := eventScope(&v.Events[i])
		if x.Fence > latest[x.RequestID] {
			latest[x.RequestID] = x.Fence
		}
	}
	// Confirmation resume needs the original input plus only the approved call.
	userIndex := -1
	if s.PreserveCallID != "" {
		for i := range v.Events {
			x := eventScope(&v.Events[i])
			if x.RequestID == s.RequestID && v.Events[i].Response != nil {
				for _, c := range v.Events[i].Choices {
					if c.Message.Role == model.RoleUser {
						userIndex = i
					}
				}
			}
		}
	}
	var events []agentevent.Event
	seen := make(map[string]bool)
	for i := range v.Events {
		e := &v.Events[i]
		x := eventScope(e)
		if e.ID != "" && seen[e.ID] {
			continue
		}
		if e.ID != "" {
			seen[e.ID] = true
		}
		oldCurrent := x.RequestID == s.RequestID && x.Fence != s.Fence
		oldOther := x.RequestID != "" && x.RequestID != s.RequestID && x.Fence < latest[x.RequestID]
		if oldOther {
			continue
		}
		copy := cloneHistoryEvent(e)
		if oldCurrent {
			if s.PreserveCallID == "" || copy.Response == nil {
				continue
			}
			var choices []model.Choice
			for _, c := range copy.Choices {
				if i == userIndex && c.Message.Role == model.RoleUser {
					choices = append(choices, c)
					continue
				}
				if c.Message.Role != model.RoleAssistant {
					continue
				}
				var calls []model.ToolCall
				for _, call := range c.Message.ToolCalls {
					if call.ID == s.PreserveCallID {
						calls = append(calls, call)
					}
				}
				if len(calls) > 0 {
					c.Message = model.Message{Role: model.RoleAssistant, ToolCalls: calls}
					choices = append(choices, c)
				}
			}
			copy.Choices = choices
			if len(choices) == 0 {
				continue
			}
		}
		events = append(events, *copy)
	}
	// Pair tools within root request, attempt and invocation branch. A result
	// from an unrelated attempt must never satisfy an orphaned call.
	key := func(e *agentevent.Event, id, name string) string {
		x := eventScope(e)
		if x.RequestID == s.RequestID && id == s.PreserveCallID {
			return "confirmation\x00" + id + "\x00" + name
		}
		return x.RequestID + "\x00" + strconv.FormatUint(x.Fence, 10) + "\x00" + e.InvocationID + "\x00" + e.Branch + "\x00" + id + "\x00" + name
	}
	calls, results := make(map[string]bool), make(map[string]bool)
	for i := range events {
		e := &events[i]
		if e.Response == nil {
			continue
		}
		for _, c := range e.Choices {
			for _, call := range c.Message.ToolCalls {
				calls[key(e, call.ID, call.Function.Name)] = true
			}
			if c.Message.Role == model.RoleTool {
				results[key(e, c.Message.ToolID, c.Message.ToolName)] = true
			}
		}
	}
	v.Events = nil
	for i := range events {
		e := &events[i]
		x := eventScope(e)
		if e.Response == nil || x.RequestID == "" || x.RequestID == s.RequestID {
			v.Events = append(v.Events, *e)
			continue
		}
		var choices []model.Choice
		for _, c := range e.Choices {
			if c.Message.Role == model.RoleTool && !calls[key(e, c.Message.ToolID, c.Message.ToolName)] {
				continue
			}
			if len(c.Message.ToolCalls) > 0 {
				var paired []model.ToolCall
				for _, call := range c.Message.ToolCalls {
					if results[key(e, call.ID, call.Function.Name)] {
						paired = append(paired, call)
					}
				}
				c.Message.ToolCalls = paired
				if len(paired) == 0 && c.Message.Content == "" && len(c.Message.ContentParts) == 0 {
					continue
				}
			}
			choices = append(choices, c)
		}
		e.Choices = choices
		if len(choices) > 0 {
			v.Events = append(v.Events, *e)
		}
	}
	// Cached tracks/summaries may contain events omitted above. Disable these
	// alternate history sources in the view; persistent SDK state is untouched.
	v.Tracks = nil
	v.Summaries = nil
	return v
}

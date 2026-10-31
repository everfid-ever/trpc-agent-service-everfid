# 故障恢复后继续对话 — 实现代码附录

## 请求与 Fence 感知的历史读取实现

来源：`trpcservice/storage/session/history.go`，完整文件。仅改变读取副本，保留 SDK 事件 ID 及原始存储；当前请求旧尝试省略，其他请求按最高观测 Fence 整理并过滤孤立工具记录。详细兼容边界见 [设计](design.md#重试历史的读取视图)。

```go
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
```

对应回归：`storage/session/history_test.go`，包括副本不修改原事件、同文本不同 request 保留、迟到旧 Fence 和批准调用精确保留。


本篇代码从当前工作区源码逐字提取；省略的文件 import/其他函数以明确的节选说明表示。每段均给出状态变化与故障含义。示例身份、测试时延与状态不是运行结果。

## 身份映射先固定租户/会话

来源：`trpcservice/channels/identity/identity.go`；输入必须是已验签 Binding；HMAC 及长度前缀编码避免账号/用户拼接碰撞，不能从正文选择 tenant。

```go
func (m Mapper) Map(ctx context.Context, binding channel.VerifiedBinding, event channel.ProviderEvent) (Result, error) {
	if m.Secrets == nil || binding.TenantID == "" || binding.ChannelBindingID == "" || binding.Channel == "" ||
		binding.ExternalAccountID == "" || binding.TenantVersion < 1 || binding.BindingVersion < 1 ||
		binding.IdentitySecretRef.Ref == "" || binding.IdentitySecretRef.Version < 1 ||
		binding.SessionSecretRef.Ref == "" || binding.SessionSecretRef.Version < 1 ||
		event.Channel != binding.Channel || event.ExternalAccountID != binding.ExternalAccountID || event.ExternalUserID == "" {
		return Result{}, runtime.ErrInvariantViolation
	}
	identityKey, err := m.resolve(ctx, binding, secrets.PurposeTenantIdentity, binding.IdentitySecretRef)
	if err != nil {
		return Result{}, err
	}
	defer zero(identityKey)
	sessionKey, err := m.resolve(ctx, binding, secrets.PurposeTenantSession, binding.SessionSecretRef)
	if err != nil {
		return Result{}, err
	}
	defer zero(sessionKey)

	userID := derive("u1_", identityKey, "identity-v1", event.Channel, event.ExternalAccountID, event.ExternalUserID)
	var sessionID string
	switch strings.ToLower(event.ConversationType) {
	case "dm", "p2p":
		sessionID = derive("s1_", sessionKey, "session-v1", event.Channel, event.ExternalAccountID, "dm", event.ExternalUserID)
	case "group":
		if event.ExternalChatID == "" {
			return Result{}, runtime.ErrInvariantViolation
		}
		sessionID = derive("s1_", sessionKey, "session-v1", event.Channel, event.ExternalAccountID, "group", event.ExternalChatID)
	default:
		return Result{}, runtime.ErrInvariantViolation
	}
	return Result{UserID: userID, SessionID: sessionID}, nil
}

func derive(prefix string, key []byte, fields ...string) string {
	mac := hmac.New(sha256.New, key)
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write([]byte(field))
	}
	return prefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
```

## SDK 的租户作用域与同步历史

来源：`trpcservice/storage/session/postgres/official.go`；TenantKey 对应真实 app_name；同步 SDK 持久化独立于平台 terminal 事务。

```go
func TenantKey(tenantID, agentAppID, userID, sessionID string) session.Key {
	return session.Key{
		AppName:   tenantID + TenantAppSep + agentAppID,
		UserID:    userID,
		SessionID: sessionID,
	}
}

// ParseTenantApp decodes tenantID / agentAppID from an official app_name.
// It returns ok=false for any malformed value (empty segment, missing
// separator, or an extra separator); callers must fail closed on ok=false.

func TenantGetSessionHook(tenantID, agentAppID string) session.GetSessionHook {
	want := tenantID + TenantAppSep + agentAppID
	return func(ctx *session.GetSessionContext, next func() (*session.Session, error)) (*session.Session, error) {
		if ctx.Key.AppName != want {
			return nil, fmt.Errorf("session tenant guard: app_name %q outside scope %q", ctx.Key.AppName, want)
		}
		return next()
	}
}

// NewOfficialSessionService builds the shared official session/postgres
// backend used by a Worker role. Tenant scope is enforced by the per-turn
// facade, not by a process-wide static hook: one Worker legitimately serves
// multiple tenant/app pairs. DB init is skipped so table creation stays under
// the platform migration authority rather than the SDK's implicit DDL.

func NewOfficialSessionService(dsn string) (*sessionpostgres.Service, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("official session postgres dsn is empty")
	}
	svc, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithEnableAsyncPersist(false),
	)
	if err != nil {
		return nil, fmt.Errorf("new official session postgres service: %w", err)
	}
	return svc, nil
}

// NewTenantScopedOfficialSessionService is retained for single-scope callers
// such as narrowly scoped maintenance jobs. Multi-tenant process roles must
// use NewOfficialSessionService and the checked BufferedTurn facade instead.
```

## fence 校准与获取

来源：`trpcservice/coordination/redis/lease.go`；租户/app/session 摘要构成 Redis hash tag；校准持久 fence 后 Acquire 才可执行。

```go
func (m *Manager) EnsureFenceAtLeast(ctx context.Context, key coordination.SessionKey, minimum uint64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	// Redis INCR is signed 64-bit, so keep one value available for the next
	// successful acquisition after calibration.
	if minimum >= math.MaxInt64 {
		return runtime.ErrInvariantViolation
	}
	_, fenceKey := m.keys(key)
	return ensureFenceScript.Run(ctx, m.client, []string{fenceKey}, minimum).Err()
}

func (m *Manager) keys(key coordination.SessionKey) (string, string) {
	digest := sha256.Sum256([]byte(key.TenantID + "\x00" + key.AgentAppID + "\x00" + key.SessionID))
	tag := hex.EncodeToString(digest[:])
	prefix := fmt.Sprintf("trpc:%s:{%s}", m.environment, tag)
	return prefix + ":lease", prefix + ":fence"
}

func (m *Manager) Acquire(ctx context.Context, key coordination.SessionKey, workerID string, ttl time.Duration) (coordination.Lease, error) {
	if err := validate(key, workerID, ttl); err != nil {
		return coordination.Lease{}, err
	}
	leaseID, err := newLeaseID()
	if err != nil {
		return coordination.Lease{}, err
	}
	leaseKey, fenceKey := m.keys(key)
	result, err := acquireScript.Run(ctx, m.client, []string{leaseKey, fenceKey}, workerID, leaseID, ttl.Milliseconds()).Slice()
	if err != nil {
		return coordination.Lease{}, err
	}
	if len(result) != 2 || asInt64(result[0]) != 1 {
		return coordination.Lease{}, runtime.ErrVersionConflict
	}
	fence, err := strconv.ParseUint(fmt.Sprint(result[1]), 10, 64)
	if err != nil || fence == 0 {
		return coordination.Lease{}, runtime.ErrInvariantViolation
	}
	return coordination.Lease{Session: key, WorkerID: workerID, LeaseID: leaseID, Fence: fence, ExpiresAt: time.Now().Add(ttl)}, nil
}
```

## 提交与历史的边界

来源：`trpcservice/storage/session/buffered_turn.go`；生产提交不复制 SDK 事件；Rollback 不撤销已写 SDK 事件。

```go
func (t *BufferedTurn) Commit(ctx context.Context, request CommitTurnRequest) (CommitTurnResult, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return CommitTurnResult{}, runtime.ErrCommitConflict
	}
	request.SessionKey = t.key
	// Session/Event/State have already been persisted through the official SDK
	// service. Do not mirror them into the platform coordination tables: that
	// would recreate a second Session source of truth. The remaining commit is
	// deliberately best-effort coordination for fence/order/outbox semantics.
	request.Events = nil
	request.StateDelta = nil
	request.SummaryCandidate = nil
	t.mu.Unlock()
	result, err := t.store.CommitTurn(ctx, request)
	if err != nil {
		return CommitTurnResult{}, err
	}
	t.mu.Lock()
	t.closed, t.committed = true, true
	t.mu.Unlock()
	return result, nil
}

func (t *BufferedTurn) Rollback(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return runtime.ErrCommitConflict
	}
	t.closed = true
	// The SDK may already have synchronously persisted events or state before
	// a later platform coordination failure. Its public API has no atomic
	// cross-store rollback hook, so rollback deliberately only discards this
	// turn's in-process coordination metadata.
	return nil
}
```

## 冻结答案续提交

来源：`trpcservice/worker/runner.go`；提交前重查取消和执行权，把 terminal 与原 reply Outbox 原子推进；不重新生成模型答案。

```go
func (w RunnerExecutor) commitFrozenSuccess(ctx context.Context, turn *sessionstore.BufferedTurn, envelope runtime.ExecutionEnvelope, head sessionstore.SessionHead, fence uint64, beforeCommit func(context.Context) error, resultRef string) error {
	latest, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	if err = verifyAuthoritativeEnvelope(latest.Envelope, envelope); err != nil {
		return err
	}
	if latest.CancelRequested {
		return runtime.ErrCancelRequested
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return err
		}
	}
	replyID, err := messaging.StableReplyID(messaging.ReplyCoordinate{
		TenantID: envelope.TenantID, RequestID: envelope.RequestID,
		InputSeq: envelope.InputSeq, Stage: "terminal", Ordinal: 0,
	})
	if err != nil {
		return err
	}
	_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{
		SessionKey: head.SessionKey, RequestID: envelope.RequestID,
		CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal",
		InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version,
		Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef, ReplyCursor: envelope.RequestID + ":1",
		Outbox: []sessionstore.OutboxEvent{
			{Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1,
				TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)},
			terminalAuditOutbox(ctx, envelope, runtime.OutcomeSucceeded),
		},
	})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	return err
}

// CancelWithLease turns a durable cancellation intent into the only
// authoritative cancelled terminal: a fenced CommitTurn that advances the
// session input gate and emits an audit fact.
```

## 排空不被成功轮询复活

来源：`cmd/trpc-service/runtime_health.go`；stopping 后忽略迟到观察，readiness 持续撤销。

```go
func (h *runtimeHealth) observe(name string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.components[name]
	if !ok || h.stopping {
		return
	}
	if err == nil {
		c.State, c.Reason, c.LastSuccess = "ready", "", time.Now().UTC()
	} else {
		// Do not expose raw dependency errors, which may contain credentials.
		c.State, c.Reason, c.LastFailure = "degraded", "operation_failed", time.Now().UTC()
		c.Failures++
	}
	h.components[name] = c
}

func (h *runtimeHealth) beginDrain() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	for name, c := range h.components {
		c.State, c.Reason = "draining", "planned_stop"
		h.components[name] = c
	}
}
```

## 结论与证据回查

字段及约束见 [design.md](design.md)，故障判定见 [testing-and-acceptance.md](testing-and-acceptance.md)。代码只能证明条件更新和恢复分支存在，真实渠道可见次数仍需客户端证据，不能从 Ledger 一行推断下游接收次数。

## 从代码回查不变量：原身份与最终目的地

身份映射代码回答“原消息属于谁”，commitFrozenSuccess回答“哪个结果能成为终态”，ReplyRelay回答“这个结果应该发往哪里”。三段分工不能被单个tenant字段替代。

| 结论 | 需要同时成立的代码条件 | 故障反例 |
|---|---|---|
| 同Bot同会话命中原历史 | HMAC输入/密钥稳定、SDK app/user/session固定 | 重启时把process ID加进session派生。 |
| 不读另一租户历史 | 可信Binding、SQL tenant约束、SDK checked facade | 外部正文直接决定tenant。 |
| 原结果不漂移 | 冻结结果身份/摘要校验、beforeCommit、terminal幂等 | 接管后覆盖同request的另一答案。 |
| 不发错Bot | 原config版本与Inbox目的地恢复route，Relay核对 | 发给当前默认Bot。 |

## 这几段代码没有承诺什么

TenantKey编码是租户作用域的一部分，不表示SDK原生拥有tenant列。BufferedTurn.Rollback不撤销SDK事件；读到助手历史并不自动等于terminal完成。fence拒绝的是平台迟到提交，不会抹去外部渠道已接受的回复。

所以检查“不重复回复”需要继续沿Delivery代码和客户端证据阅读。只审查commit函数会遗漏P4；只审查发送函数又会遗漏输入和会话身份漂移。

## 原持久字段恢复ReplyRoute的实际查询

来源：trpcservice/storage/messaging/postgres/store.go。这里没有独立reply_route表，也没有模型提供的目标账号；查询使用原execution配置版本与Inbox目的地。原Binding版本不可读会失败，不回退最新默认Bot。

```go
func (s *Store) ResolveReplyRoute(ctx context.Context, tenantID, requestID string) (messaging.ReplyRoute, error) {
	if tenantID == "" || requestID == "" {
		return messaging.ReplyRoute{}, runtime.ErrTenantScope
	}
	var route messaging.ReplyRoute
	route.TenantID, route.RequestID = tenantID, requestID
	err := s.db.QueryRowContext(ctx, `SELECT e.channel,e.config_version,cb.binding_id,i.external_account_id,i.external_message_id,i.external_chat_id,i.external_user_id
FROM execution_record e
JOIN inbox i ON i.tenant_id=e.tenant_id AND i.request_id=e.request_id
JOIN channel_binding cb ON cb.tenant_id=e.tenant_id AND cb.config_version=e.config_version
  AND cb.channel=i.channel AND cb.external_account_id=i.external_account_id
  AND cb.agent_app_id=e.agent_app_id
WHERE e.tenant_id=$1 AND e.request_id=$2`, tenantID, requestID).
		Scan(&route.Channel, &route.ConfigVersion, &route.ChannelBindingID, &route.ExternalAccountID, &route.ExternalMessageID,
			&route.ExternalChatID, &route.ExternalUserID)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ReplyRoute{}, runtime.ErrNotFound
	}
	return route, err
}
```

## 从冻结结果构造ReplyEvent的实际校验

来源：trpcservice/relay/reply.go。结果、Outbox与路由必须在tenant/request/ref上匹配。P3暂停在事件已构造、PublishReply之前；之后的重复发布由DeliveryKey与Ledger吸收。

```go
func (r ReplyRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.Kind != "" && record.Kind != "reply" {
		return runtime.ErrInvariantViolation
	}
	result, err := messaging.ResolveReplyContent(ctx, r.Results, record.TenantID, record.AggregateID, record.PayloadRef)
	if err != nil {
		if errors.Is(err, runtime.ErrVersionMismatch) {
			return runtime.ErrTenantScope
		}
		return err
	}
	if result.TenantID != record.TenantID || result.RequestID != record.AggregateID || result.ResultRef != record.PayloadRef {
		return runtime.ErrTenantScope
	}
	route, err := r.Routes.ResolveReplyRoute(ctx, record.TenantID, record.AggregateID)
	if err != nil {
		return err
	}
	if route.TenantID != record.TenantID || route.RequestID != record.AggregateID || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.ConfigVersion < 1 {
		return runtime.ErrTenantScope
	}
	destination := channel.ReplyDestination{TenantID: route.TenantID, Channel: route.Channel, ChannelBindingID: route.ChannelBindingID,
		ExternalAccountID: route.ExternalAccountID, ConfigVersion: route.ConfigVersion}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: record.TenantID, RequestID: record.AggregateID,
		ChannelBindingID: route.ChannelBindingID, DeliveryKey: record.IdempotencyKey, ConfigVersion: route.ConfigVersion,
		EventSeq: record.EventSeq, Kind: "message.completed", ContentRef: result.ResultRef, ContentType: result.ContentType,
		Target: channel.DeliveryTarget{Channel: route.Channel, ExternalAccountID: route.ExternalAccountID,
			ExternalMessageID: route.ExternalMessageID, ExternalChatID: route.ExternalChatID, ExternalUserID: route.ExternalUserID},
		Final: true, TraceParent: telemetry.EffectiveTraceParent(ctx, record.TraceParent)}
	if r.Barrier != nil {
		if err := r.Barrier.Wait(inflight.WithScope(ctx, event.TenantID, event.RequestID, ""), inflight.PointP3BeforeReplyPublish); err != nil {
			return err
		}
	}
	return r.Replies.PublishReply(ctx, destination, event)
}
```

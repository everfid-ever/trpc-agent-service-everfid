# 故障恢复后继续对话 — 实现代码附录

> 本文直接摘录关键生产代码，并解释每段代码**改变了什么状态、为何能抵抗单节点故障**。省略的仅是无关初始化、凭据与用户数据。如需与原实现交叉核对，可见各节标注的源文件位置。

---

## 1. 实例身份：部署槽位与本次进程必须分开

**目标代码位置（可选核对）：** `cmd/trpc-service/webui_local_role.go`。

```go
func (value webUILocalConfig) instanceName(component string) string {
	return "webui-local-" + component + "-" + value.InstanceID + "-" + value.ProcessStartID
}

type webUILocalRuntimeStatus struct {
	InstanceID      string            `json:"instance_id"`
	ProcessStartID  string            `json:"process_start_id"`
	Owners          map[string]string `json:"owners"`
	InflightBarrier inflight.Status   `json:"inflight_test_barrier"`
}

func newWebUILocalProcessStartID() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
```

**改变了什么状态 / 为何抗故障：** `InstanceID` 是 N1/N2 等固定部署槽位；`ProcessStartID` 是每次启动随机生成的实例代号（8 字节 hex）。两者共同进入 worker、relay、preprocess、delivery 的 owner/consumer 名称。这样 N1 强杀并重新加入后，不会被误认为是“仍然持有旧租约的那一个进程”（不变量 I9）。`/statusz` 只暴露这些匿名运行标识，供把租约、消费者和日志映射到真实进程，不暴露租户或消息内容。

---

## 2. 第二 Bot 的完整性与可区分性校验

```go
if value.WeComSecondaryEnabled {
	agentID, agentIDErr := strconv.ParseInt(
		strings.TrimSpace(getenv("WECOM_SECONDARY_AGENT_ID")), 10, 64)
	if agentIDErr != nil || agentID <= 0 || value.WeComSecondaryCorpID == "" ||
		value.WeComSecondaryAppSecret == "" || value.WeComSecondaryCallbackToken == "" ||
		value.WeComSecondaryEncodingAESKey == "" {
		return webUILocalConfig{}, errors.New("secondary WeCom local configuration is incomplete")
	}
	value.WeComSecondaryAgentID = agentID
	if !value.WeComEnabled ||
		(value.WeComSecondaryCorpID == value.WeComCorpID &&
			value.WeComSecondaryAgentID == value.WeComAgentID) {
		return webUILocalConfig{}, errors.New("secondary WeCom local configuration is incompatible")
	}
}
```

**改变了什么状态 / 为何抗故障：** 避免“只配置了一半的第二 Bot”以及“用相同 Corp 与相同 Agent 假装有两个 Bot”。成功启动后，两份可信 binding 分别推导 tenant、Agent、Secret 和会话查询范围；外部请求体中的 tenant 字段不参与可信路由（不变量 I1，身份不串的根因）。

---

## 3. readiness：活着不代表能处理业务

```go
mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
})
mux.HandleFunc("/statusz", func(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(writer).Encode(configValue.runtimeStatus(inflightBarrier))
})
mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
	if db.PingContext(request.Context()) != nil ||
		redis.Ping(request.Context()).Err() != nil || malware.Probe(request.Context()) != nil {
		http.Error(writer, "not ready", http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
})
```

**改变了什么状态 / 为何抗故障：** `/livez` 只表示进程没有退出；`/readyz` 同时要求数据库、队列协调服务和必需依赖（malware 探针）可用。稳定入口只转发到 ready 实例，因此“进程仍在但无法读会话/写队列”的节点不会继续吞掉 callback（不变量 I10）。入口摘除是故障接管链路的第一环（`t_connect` 的前置）。

---

## 4. 稳定入口：只转发健康后端，不替代业务幂等

**目标代码位置（可选核对）：** `cmd/trpc-service/wecom_ha_entry_role.go`。

关键语义（逐字取自实现）：
- `probe()`：对每个 backend 请求 `<backend>/readyz`；HTTP 200 → healthy，否则 false。
- `healthyBackends()`：从原子轮转下标开始按序返回健康 backend（round-robin 起点）。
- `serveCallback()`：仅 GET/POST，否则 405；`http.MaxBytesReader(limit)`（`RequestLimit = 2<<20` = 2 MiB）超限 → 413；按健康 backend 顺序 forward；forward 出错或返回 502/503/504 → 标记不健康并试下一个；全部失败 → 503 `no ready callback backend`。
- 自身 `/livez` 恒 200；`/readyz` 仅至少一个健康 backend 才 200；`/statusz` 输出 `{"backends":[{"url":...,"healthy":bool}]}`。
- **入口重试不替代业务幂等**：只在本次 callback 尚未返回响应前换后端；真正去重仍靠后端 durable Inbox（`claim_inbox`）。

**为何抗故障：** 公开 callback 地址不随实例切换，WeCom 控制台只配置一个 origin；任一后端 SIGKILL 后，入口探测到 `/readyz` 失败即摘除，callback 自动转给存活后端（不变量 I8）。

---

## 5. Worker：先读持久化 fence，再获得会话租约

**目标代码位置（可选核对）：** `trpcservice/worker/consumer.go:181`。

```go
func (w Consumer) handle(ctx context.Context, delivery broker.Delivery) error {
	key := coordination.SessionKey{
		TenantID: delivery.Envelope.TenantID, AgentAppID: delivery.Envelope.AgentAppID, SessionID: delivery.Envelope.SessionID,
	}
	persistedFence, err := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
	if err != nil {
		return err
	}
	if err := w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence); err != nil {
		return err
	}
	lease, err := w.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Leases.Release(releaseCtx, lease)
	}()
	// 续租、执行、fence 提交和 ACK 在后续步骤完成。
	return nil
}
```

**改变了什么状态 / 为何抗故障：** 这个顺序让数据库的 `last_fence` 成为最终裁决值。`EnsureFenceAtLeast` 在协调服务（Redis）计数因重启回退时，先校准到持久化值再发放更大的 lease fence。N1 失联、N2 接管并提交后，N1 恢复网络时只能携带更小 fence，数据库会拒绝它的迟到提交（不变量 I3）。

续租 goroutine（`consumer.go:217`）：

```go
go func() {
	defer close(renewalDone)
	ticker := time.NewTicker(w.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopRenewal:
			return
		case <-executionCtx.Done():
			return
		case <-ticker.C:
			if _, renewErr := w.Leases.Renew(executionCtx, lease, w.LeaseTTL); renewErr != nil {
				markLeaseLost()
				return
			}
		}
	}
}()
```

**为何抗故障：** 续租失败 → `markLeaseLost()`（一次性关闭 `leaseLost` 并取消执行 context），保证“失去执行权即停止提交”。

---

## 6. lease 失效就停止，terminal commit 后才 ACK

```go
beforeCommit := func(commitCtx context.Context) error {
	select {
	case <-leaseLost:
		return runtime.ErrLeaseLost
	default:
	}
	if _, renewErr := w.Leases.Renew(commitCtx, lease, w.LeaseTTL); renewErr != nil {
		markLeaseLost()
		return runtime.ErrLeaseLost
	}
	return nil
}

executeErr = w.Executor.ExecuteWithLease(
	executionCtx, delivery.Envelope, lease.Fence, beforeCommit)
if executeErr != nil {
	return executeErr // 不 ACK；保留 pending 给存活节点 reclaim
}
return w.Broker.Ack(ctx, delivery)
```

**改变了什么状态 / 为何抗故障：** 续租失败会取消执行上下文；提交前再续租一次；失败任务不会 ACK。三者共同保证“失去执行权的节点不能提交、未完成消息仍可被 N2 回收”（I5）。注意 `ErrInputNotReady` 走 `Parker.ParkInput`（停放逻辑），`ErrCancelRequested` 走 `CancelWithLease`——都不绕过 ACK 规则。

---

## 7. Redis 租约与 fence 契约（逐字 Lua）

**目标代码位置（可选核对）：** `trpcservice/coordination/redis/lease.go`。

键格式（Go `keys()`）：
```go
digest := sha256.Sum256([]byte(key.TenantID + "\x00" + key.AgentAppID + "\x00" + key.SessionID))
tag := hex.EncodeToString(digest[:])
prefix := fmt.Sprintf("trpc:%s:{%s}", m.environment, tag)
// leaseKey = prefix + ":lease"   fenceKey = prefix + ":fence"
// lease value = workerID + "|" + leaseID + "|" + fence
```

```lua
-- acquire
if redis.call('EXISTS', KEYS[1]) == 1 then return {0, '0'} end
redis.call('INCR', KEYS[2])
local fence = redis.call('GET', KEYS[2])
local value = ARGV[1] .. '|' .. ARGV[2] .. '|' .. fence
redis.call('PSETEX', KEYS[1], ARGV[3], value)
return {1, fence}

-- renew
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2]); return 1

-- release
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])

-- ensureFence（十进制字符串比较，current < minimum 时 SET minimum）
local function normalize(value) local n = string.gsub(value, '^0+', ''); if n == '' then return '0' end; return n end
local function decimal_less(left, right)
  left = normalize(left); right = normalize(right)
  if string.len(left) ~= string.len(right) then return string.len(left) < string.len(right) end
  return left < right
end
local current = redis.call('GET', KEYS[1]) or '0'
local minimum = ARGV[1]
if decimal_less(current, minimum) then redis.call('SET', KEYS[1], minimum) end
return 1
```

**为何抗故障：** `acquire` 冲突返回 `ErrVersionConflict`；fence 解析失败/为 0 → `ErrInvariantViolation`。`Renew`/`Release` 必须三者（owner、leaseID、fence）整体比对（值串比较），防止误释放他人 lease。`EnsureFenceAtLeast` 拒绝 `minimum >= math.MaxInt64`（Redis INCR 是 signed 64-bit，留一个可用值）。`workerID` 不得为空、不得含 `|`；`ttl` 必须 > 0 且 ≥1ms。

---

## 8. terminal result、会话推进和待发回复原子提交

```go
replyID, err := messaging.StableReplyID(messaging.ReplyCoordinate{
	TenantID: envelope.TenantID, RequestID: envelope.RequestID,
	InputSeq: envelope.InputSeq, Stage: "terminal", Ordinal: 0,
})
if err != nil {
	return err
}
_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{
	SessionKey: sessionKey, RequestID: envelope.RequestID,
	CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal",
	InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version,
	Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef,
	ReplyCursor: envelope.RequestID + ":1",
	Outbox: []sessionstore.OutboxEvent{
		{Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1},
	},
})
if errors.Is(err, runtime.ErrAlreadyTerminal) {
	return nil
}
```

**改变了什么状态 / 为何抗故障：** `RequestID + InputSeq + ExpectedVersion + Fence` 同时限制一次有效提交；`replyID` 对相同 tenant/request/轮次稳定不变（由 `StableReplyID` 推导）。结果、会话历史和待发回复在同一事务中成为事实，所以发送节点在 P3 死亡时，新节点只需发送保存好的结果，不应重跑模型（I4）。

对应 SQL（逐字，`commit_turn` 行 421）核心拒绝逻辑：
```sql
IF p_fence < v_head.last_fence THEN RAISE EXCEPTION 'stale fence' USING ERRCODE = '40001'; END IF;
...
UPDATE public.session_head SET version = v_new_version,
  last_fence = GREATEST(last_fence, p_fence), last_session_seq = v_new_last_seq,
  next_input_seq = next_input_seq + CASE WHEN p_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN 1 ELSE 0 END,
  state_json = state_json || COALESCE(p_state_delta, '{}'::jsonb), updated_at = now()
  WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id;
```

---

## 9. 投递台账：claim 后才调用下游，P3/P4 对账

**目标代码位置（可选核对）：** `trpcservice/channels/delivery/service.go:109`。

```go
func (s Service) deliverSegment(ctx context.Context, event channel.ReplyEvent, adapter channel.Adapter, plan messaging.DeliveryPlan, segmentNo int, content []byte, contentType string) error {
	key := messaging.DeliveryKey{TenantID: event.TenantID, DeliveryKey: event.DeliveryKey, SegmentNo: segmentNo}
	claimTTL := s.ClaimTTL
	if claimTTL <= 0 { claimTTL = 30 * time.Second }
	record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
	if err != nil { return err }
	if !acquired {
		switch record.State {
		case messaging.DeliverySent, messaging.DeliveryFailed: return nil
		case messaging.DeliveryAmbiguous: return s.reconcile(ctx, event, record)
		case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
			return DeferredError{NotBefore: record.NotBefore}
		default: return runtime.ErrInvariantViolation
		}
	}
	if s.InflightBarrier != nil {
		_ = s.InflightBarrier.Wait(ctx, inflight.Observation{Point: inflight.PointP3, TenantID: event.TenantID, RequestID: event.RequestID, Owner: s.Owner, At: time.Now().UTC()})
	}
	// ... deliverWithClaimRenewal（后台按 claimTTL/3 续租 claim）...
	if resultDelivery.ProviderMessageID == "" {
		record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "missing_provider_message_id"
		_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
		return errors.Join(AmbiguousError{Err: runtime.ErrInvariantViolation}, finishErr)
	}
	if s.InflightBarrier != nil {
		_ = s.InflightBarrier.Wait(ctx, inflight.Observation{Point: inflight.PointP4, TenantID: event.TenantID, RequestID: event.RequestID, Owner: s.Owner, At: time.Now().UTC()})
	}
	record.State = messaging.DeliverySent
	record.ProviderMessageID = resultDelivery.ProviderMessageID
	record.LastErrorClass = ""
	_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	return err
}
```

**改变了什么状态 / 为何抗故障：** Claim 成功后 `state='sending'` 并写入 `claim_owner/claim_until`；P3 暂停点验证“claim 后未发送”时由存活节点接管；P4 暂停点验证“下游已受理但未确认”时由 `client_request_id`（由 `StableDeliveryRequestID(key)` 稳定推导）去重收敛。两个发送者不能同时发同一片段（I6），下游不可去重时诚实 `ambiguous`（I7）。台账每 segment 一行，`sent` 后不可重发。

---

## 10. inbox 去重与 Provider 重试边界（逐字 SQL）

**目标代码位置（可选核对）：** `migrations/000001_service_schema.up.sql` 行 314。

```sql
INSERT INTO public.inbox(tenant_id, channel, external_account_id, external_message_id, request_id,
  agent_app_id, session_id, state, payload_ref, payload_digest, key_version)
VALUES (...) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
SELECT * INTO v_row FROM public.inbox WHERE ... FOR UPDATE;
IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
   OR v_row.agent_app_id <> p_agent_app_id OR v_row.session_id IS DISTINCT FROM p_session_id THEN
  RAISE EXCEPTION 'inbox idempotency collision' USING ERRCODE = '23505';
END IF;
```

**为何抗故障：** 平台重传 → 命中唯一键 → 读回既有行；仅当属主/摘要一致才认作同一输入；不一致视为冲突（23505）。这是 Provider 层去重边界，保证每个 provider 输入只有一个 durable 事实（I2）。

---

## 11. 可信入口：候选令牌的一次性验签（身份不串的代码证据）

**目标代码位置（可选核对）：** `trpcservice/channels/ingress/ingress.go`。

候选状态常量与四个阶段：

```go
const PurposeChannelVerify = "channel_verify"

type CandidateState string
const (
	CandidateIssued           CandidateState = "issued"
	CandidateVerifierAcquired CandidateState = "verifier_acquired"
	CandidateVerified         CandidateState = "verified"
	CandidatePromoted         CandidateState = "promoted"
	CandidateBurned           CandidateState = "burned"
)
```

`ResolveCandidate` 只返回**不透明**的候选上下文，**不含** tenant / agent / 凭据：

```go
func (r Resolver) ResolveCandidate(ctx context.Context, hint channel.PublicRouteHint) (channel.CandidateBindingContext, error) {
	if r.Store == nil || hint.Channel == "" || hint.RouteKeyDigest == "" || hint.IngressAttemptID == "" {
		return channel.CandidateBindingContext{}, runtime.ErrInvariantViolation
	}
	route, err := r.Store.ResolveBindingRoute(ctx, hint.Channel, hint.RouteKeyDigest)
	if err != nil { return channel.CandidateBindingContext{}, err }
	if !route.Enabled { return channel.CandidateBindingContext{}, runtime.ErrVersionMismatch }
	token, err := r.token()
	...
	ttl := r.TTL
	if ttl <= 0 { ttl = 30 * time.Second }
	candidate := channel.CandidateBindingContext{
		Channel: route.Channel, RouteKeyDigest: route.RouteKeyDigest, CandidateToken: token,
		BindingVersion: route.BindingVersion, Purpose: PurposeChannelVerify,
		IssuedAt: now, ExpiresAt: now.Add(ttl),
	}
	record := CandidateRecord{
		TokenDigest: digest(token), OpaqueBindingID: route.OpaqueBindingID, Channel: route.Channel,
		RouteKeyDigest: route.RouteKeyDigest, Purpose: candidate.Purpose,
		BindingVersion: route.BindingVersion, State: CandidateIssued,
		IssuedAt: candidate.IssuedAt, ExpiresAt: candidate.ExpiresAt,
	}
	if err := r.Store.IssueCandidate(ctx, record); err != nil {
		return channel.CandidateBindingContext{}, err
	}
	return candidate, nil
}
```

`AcquireVerifier` 的交叉核验与失败即 burn：

```go
record, route, err := r.Store.AcquireCandidate(ctx, digest(candidate.CandidateToken), candidate.BindingVersion, now)
if err != nil { return nil, err }
if record.Channel != candidate.Channel || record.RouteKeyDigest != candidate.RouteKeyDigest ||
	record.Purpose != candidate.Purpose || record.BindingVersion != candidate.BindingVersion ||
	!sameInstant(record.IssuedAt, candidate.IssuedAt) || !sameInstant(record.ExpiresAt, candidate.ExpiresAt) {
	_ = r.Store.BurnCandidate(context.Background(), record.TokenDigest, record.Version)
	return nil, runtime.ErrVersionMismatch
}
secret, err := r.Secrets.Resolve(ctx, secrets.Scope{
	TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
	ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion,
}, route.SecretRef)
if err != nil { _ = r.Store.BurnCandidate(...); return nil, err }
if secret.Version != route.SecretRef.Version || len(secret.Bytes) == 0 {
	_ = r.Store.BurnCandidate(...); return nil, runtime.ErrVersionMismatch
}
```

**改变了什么状态 / 为何抗故障：** 候选从 `issued` 走到 `verifier_acquired`；**tenant 只在 `PromoteVerified` 之后才被带出**。因此路由信息（公开可探测）不构成租户信息泄露，且请求体中的 `tenant_id` 从不参与路由（不变量 I1）。任一交叉核验失败即 `BurnCandidate`，保证候选不会被复用。

`verifierHandle.Verify` 的单次使用与密钥归零：

```go
func (h *verifierHandle) Verify(ctx context.Context, request channel.CallbackRequest, verify channel.ProtocolVerifier) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || verify == nil || len(h.secret) == 0 {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrVersionConflict
	}
	material := append([]byte(nil), h.secret...)
	payload, err := verify(ctx, request, material)
	zero(material)
	zero(h.secret)
	h.secret = nil
	h.closed = true
	if err != nil || payload.ProtocolIdentityDigest == "" {
		_ = h.store.BurnCandidate(context.Background(), h.record.TokenDigest, h.record.Version)
		if err == nil { err = runtime.ErrVersionMismatch }
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	receiptToken, err := h.token()
	...
	record, err := h.store.MarkCandidateVerified(ctx, h.record.TokenDigest, h.record.Version,
		digest(receiptToken), payload.ProtocolIdentityDigest, verifiedAt)
	if err != nil {
		_ = h.store.BurnCandidate(...)
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	...
}
```

**为何如此：** 验签密钥用后**立即清零并置 nil**，配合 `closed = true`，使同一候选不可能被第二次验签（重放攻击面收敛到"一次"）。未完成验证的 `Close()` 会 `BurnCandidate`，不留孤儿候选。`digest()` 使用 `sha256` + `base64.RawURLEncoding`，与候选表 `candidate_token_digest` 的字符集约束（长度 16..256）一致。

---

## 12. 会话存储的错误映射与只读历史加载

**目标代码位置（可选核对）：** `trpcservice/storage/session/postgres/store.go`。

```go
type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) { return runtime.ErrNotFound }
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			if strings.Contains(strings.ToLower(err.Error()), "stale fence") { return runtime.ErrStaleFence }
			return runtime.ErrVersionConflict
		case "23505": return runtime.ErrCommitConflict
		case "XX001": return runtime.ErrInvariantViolation
		case "55000": return runtime.ErrInputNotReady
		case "P0902": return runtime.ErrCancelRequested
		case "42501": return runtime.ErrTenantScope
		}
	}
	return err
}
```

**为何必须如此：** `ErrVersionConflict` / `ErrCommitConflict` 是**可重试竞争**（`Consumer.process` 会按 `RetryWait` 重试），而 `ErrInvariantViolation` 必须上抛并告警。把两者混为一类会导致"数据不一致被当作竞争而无限重试"，掩盖真实故障。特别是 `40001` 依赖错误消息中的 `stale fence` 字样来区分"迟到写被拒"与普通版本冲突——**这是唯一能区分二者的信号，若遗失该字样，fence 机制将退化为普通乐观锁。**

`OpenForRun` 的 JOIN 校验（必须能证明该 request/input_seq 属于该会话）：

```sql
SELECT h.version,h.last_fence,h.last_session_seq,h.next_input_seq,h.state_json
FROM session_head h
JOIN execution_record e
  ON e.tenant_id=h.tenant_id AND e.agent_app_id=h.agent_app_id AND e.session_id=h.session_id
WHERE h.tenant_id=$1 AND h.agent_app_id=$2 AND h.session_id=$3
  AND e.request_id=$4 AND e.input_seq=$5
```

随后按顺序判定：`input_seq < next_input_seq` → `ErrAlreadyTerminal`；`>` → `ErrInputNotReady`；`fence < last_fence` → `ErrStaleFence`。

`LoadSession` 只读平台协作表里**已提交**的事件（**注意：生产 durable turn 不向该表写行，见下方说明**）：

```sql
SELECT event_payload FROM session_event
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3
ORDER BY session_seq
```

**改变了什么状态 / 为何抗故障：** 只读，无状态改变。这里必须澄清一处容易被写错的事实：**"接管后的上下文来自哪里"的答案是官方会话后端的 `session_events`（复数，`app_name = tenantID + "/" + agentAppID`，由 `trpc-agent-go/session/postgres` 同步写入），而不是平台 `session_event`（单数）**。生产 worker 使用 `NewDurableBufferedTurnScoped`，其 `Commit` 显式把 `Events`/`StateDelta`/`SummaryCandidate` 置 nil（避免重建第二个会话真相源），因此 `commit_turn` 的 `p_events` 为 null、平台 `session_event` 不产生新行，`LoadSession` 也没有生产调用方——它只服务测试与迁移路径。结论：N1 内存全失后 N2 之所以能答对代号，是因为**模型上下文可从官方会话后端重建**（每次尝试追加的事件都同步落库），事件顺序即上下文顺序。详见 [../DATABASE-DESIGN.md](../DATABASE-DESIGN.md) §1.1 与 [../inflight-task-takeover/REFERENCE-IMPLEMENTATION.md](../inflight-task-takeover/REFERENCE-IMPLEMENTATION.md) §10.1。

---

## 13. Relay：Outbox → 可重放工作（装配值）

**目标代码位置（可选核对）：** `cmd/trpc-service/webui_local_role.go:386-401`。

```go
dispatchRelay := relay.DispatchRelay{Outbox: inbox, Tasks: tasks, Broker: streamBroker,
	Owner: configValue.instanceName("dispatch-relay"),
	ShardCount: 4, ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second,
	PollInterval: 100 * time.Millisecond, Telemetry: telemetryProvider}
replyRelay := relay.ReplyRelay{Outbox: inbox, Results: payloads, Routes: inbox, Replies: publisher,
	Owner: configValue.instanceName("reply-relay"), ClaimTTL: 30 * time.Second,
	ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond, Telemetry: telemetryProvider}
wakeupRelay := relay.WakeupRelay{Outbox: inbox, Wakeups: publisher,
	Owner: configValue.instanceName("wakeup-relay"), ClaimTTL: 30 * time.Second,
	ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond, Telemetry: telemetryProvider}
wakeupDispatcher := relay.WakeupDispatcher{ConsumerID: configValue.instanceName("wakeup"),
	Wakeups: wakeupQueue, Store: tasks, Dispatch: streamBroker, ShardCount: 4,
	ReclaimInterval: 5 * time.Second, ReclaimLimit: 100}
```

Redis 队列配置：

```go
wakeupQueue, err := relayredis.NewWakeupQueue(redis, publisher, relayredis.WakeupQueueConfig{
	Group: "webui-wakeup", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
replyQueue, err := relayredis.NewReplyQueue(redis, publisher, relayredis.ReplyQueueConfig{
	Group: "webui-delivery", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
```

**为何抗故障：** 每个 relay 的 owner 都含 `ProcessStartID`，且都有 `ClaimTTL` + `ClaimRenewInterval`。发布者是外部副作用，若直接在 `commit_turn` 事务里做，失败会导致已提交结果丢失；若直接无 claim 地发，两节点会重复发。Outbox + claim + 幂等键（`(tenant_id, kind, idempotency_key)` 唯一约束）三者组合，使发布成为"至少一次但收敛"的操作（不变量 I4 的延伸）。

---

## 14. 代码与设计结论的对应关系

| 设计结论 | 本文代码证据 |
|---|---|
| 重新加入不会继承旧 owner | §1 进程启动身份（ProcessStartID） |
| 两个 Bot 不能被错误合并 | §2 第二 Bot 身份校验，后续 binding 路由 |
| 入口只选可服务节点 | §3 readiness；§4 稳定入口探测 |
| tenant 不来自请求体 | §11 候选令牌四阶段；tenant 仅由 `PromoteVerified` 带出 |
| 候选不可重放 | §11 验签密钥用后归零 + `closed` 标记 + 失败即 burn |
| 旧节点不能覆盖新结果 | §5/§6 lease/fence/commit 前续租；§8 stale fence 拒绝 |
| 原会话和回复可恢复 | §8 原子 terminal outbox；§9 台账 CAS |
| 投递不双发 | §9 Delivery Ledger 条件更新 |
| 入站不重复处理 | §10 claim_inbox 唯一键 + 冲突拒绝 |
| 接管后上下文来自持久化历史 | §12 `LoadSession` 按 `session_seq` 只读已提交事件 |
| 错误必须区分"竞争"与"不一致" | §12 `translate()` 的 SQLSTATE 映射 |
| 发布是至少一次但收敛 | §13 Relay 装配值 + outbox 幂等约束 |

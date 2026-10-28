# 在途任务接管与幂等投递 — 参考实现

## 复刻前提、迁移与实现范围

本文给出本能力的实际表结构、SQL/Go 算法和配置契约。字段表优先见 design；本篇保留逐字 DDL。SQL 按依赖创建，以下是可靠性子系统摘录，不能作为一段无依赖 SQL 直接执行：tenant、agent_app、版本化配置/策略、授权等被引用的控制面表必须先存在。仓库部署使用完整迁移 000001，再依次 000002、000003、000004；不要把本文摘录再次应用到已迁移库。

完整服务还需要渠道协议验签、SDK PostgreSQL backend、租户密钥解析器、Redis broker、版本化配置加载和真实模型/provider 适配。SDK 表由迁移创建，SDK WithSkipDBInit(true)、WithEnableAsyncPersist(false)，禁止启动时另造 schema。本文是当前能力的复刻契约；不会把省略的基础设施宣称为“复制一段示例即可启动全部系统”。

## 数据结构原文

本篇含在途新增表；Inbox、session_head/commit、SDK事件和输入/结果 DDL 见同级 [会话参考实现](../conversation-continuity/REFERENCE-IMPLEMENTATION.md#数据结构原文)。下方execution_record已包括000004新增列。

### preprocess_job

```sql
CREATE TABLE public.preprocess_job (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    job_id text NOT NULL,
    tenant_version bigint NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    channel text NOT NULL,
    payload_ref text NOT NULL,
    traceparent text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    lease_owner text,
    lease_until timestamp with time zone,
    not_before timestamp with time zone DEFAULT now() NOT NULL,
    reject_reason text DEFAULT ''::text NOT NULL,
    dispatched_at timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    prepared_payload_ref text DEFAULT ''::text NOT NULL,
    channel_binding_id text NOT NULL,
    config_version bigint NOT NULL,
    CONSTRAINT preprocess_job_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT preprocess_job_check CHECK ((((lease_owner IS NULL) AND (lease_until IS NULL) AND (state <> 'running'::text)) OR ((lease_owner IS NOT NULL) AND (lease_until IS NOT NULL) AND (state = ANY (ARRAY['running'::text, 'ready'::text]))))),
    CONSTRAINT preprocess_job_check1 CHECK (((dispatched_at IS NULL) OR (state = 'ready'::text))),
    CONSTRAINT preprocess_job_config_version_check CHECK ((config_version >= 1)),
    CONSTRAINT preprocess_job_prepared_ref_complete CHECK ((((state <> 'ready'::text) AND (prepared_payload_ref = ''::text)) OR ((state = 'ready'::text) AND ((prepared_payload_ref = ''::text) OR (length(btrim(prepared_payload_ref)) > 0))))),
    CONSTRAINT preprocess_job_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'ready'::text, 'rejected'::text, 'retry_wait'::text]))),
    CONSTRAINT preprocess_job_tenant_version_check CHECK ((tenant_version >= 1)),
    CONSTRAINT preprocess_job_version_check CHECK ((version >= 0))
);
ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_pkey PRIMARY KEY (tenant_id, job_id);
ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_tenant_id_request_id_key UNIQUE (tenant_id, request_id);
ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_channel_binding_fk FOREIGN KEY (tenant_id, config_version, channel_binding_id) REFERENCES public.channel_binding(tenant_id, config_version, binding_id);
ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);
CREATE INDEX preprocess_job_claim_idx ON public.preprocess_job USING btree (state, not_before, lease_until, created_at);
CREATE INDEX preprocess_job_ready_idx ON public.preprocess_job USING btree (created_at) WHERE ((state = 'ready'::text) AND (dispatched_at IS NULL));
```

### execution_record

```sql
CREATE TABLE public.execution_record (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    tenant_version bigint NOT NULL,
    agent_app_id text NOT NULL,
    agent_app_version bigint NOT NULL,
    agent_app_revision bigint NOT NULL,
    agent_content_digest text NOT NULL,
    config_version bigint NOT NULL,
    policy_version bigint NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    channel text NOT NULL,
    input_seq bigint NOT NULL,
    payload_ref text NOT NULL,
    traceparent text,
    outcome text DEFAULT 'queued'::text NOT NULL,
    result_ref text,
    park_attempt integer DEFAULT 0 NOT NULL,
	model_call_count integer DEFAULT 0 NOT NULL,
    not_before timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    park_deadline timestamp with time zone,
    blocked_at timestamp with time zone,
    blocked_reason text,
    cancel_requested_at timestamp with time zone,
    cancel_version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT execution_record_agent_content_digest_check CHECK ((agent_content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT execution_record_cancel_intent_check CHECK (((cancel_version >= 0) AND (((cancel_requested_at IS NULL) AND (cancel_version = 0)) OR ((cancel_requested_at IS NOT NULL) AND (cancel_version >= 1))))),
    CONSTRAINT execution_record_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT execution_record_outcome_check CHECK ((outcome = ANY (ARRAY['queued'::text, 'running'::text, 'pending'::text, 'blocked'::text, 'waiting_confirmation'::text, 'succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]))),
    CONSTRAINT execution_record_park_attempt_check CHECK ((park_attempt >= 0)),
	CONSTRAINT execution_record_model_call_count_check CHECK ((model_call_count >= 0)),
    CONSTRAINT execution_record_park_state_check CHECK (((park_attempt >= 0) AND ((park_deadline IS NULL) OR (not_before IS NULL) OR (not_before <= park_deadline)) AND ((outcome <> 'blocked'::text) OR ((blocked_at IS NOT NULL) AND (blocked_reason = ANY (ARRAY['park_attempts_exhausted'::text, 'park_deadline_exceeded'::text])))))),
    CONSTRAINT execution_record_version_check CHECK ((version >= 0))
);
ALTER TABLE public.execution_record ADD COLUMN model_request_count bigint NOT NULL DEFAULT 0 CHECK(model_request_count>=0);
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_pkey PRIMARY KEY (tenant_id, request_id);
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_agent_app_id_session_id_input_se_key UNIQUE (tenant_id, agent_app_id, session_id, input_seq);
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_agent_app_id_agent_app_revision_fkey FOREIGN KEY (tenant_id, agent_app_id, agent_app_revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision);
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_config_version_fkey FOREIGN KEY (tenant_id, config_version) REFERENCES public.config_snapshot(tenant_id, config_version);
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);
CREATE INDEX execution_park_ready_idx ON public.execution_record USING btree (tenant_id, agent_app_id, session_id, input_seq, not_before) WHERE (outcome = 'pending'::text);
```

### execution_attempt

```sql
CREATE TABLE public.execution_attempt (
 tenant_id text NOT NULL, request_id text NOT NULL, attempt_no bigint NOT NULL CHECK(attempt_no>0),
 worker_id text NOT NULL CHECK(length(btrim(worker_id))>0), fence bigint NOT NULL CHECK(fence>0),
 state text NOT NULL CHECK(state IN ('running','completed','retryable','superseded')),
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 CHECK((state='running' AND finished_at IS NULL) OR (state<>'running' AND finished_at IS NOT NULL)),
 PRIMARY KEY(tenant_id,request_id,attempt_no),
 FOREIGN KEY(tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);
```

### outbox

```sql
CREATE TABLE public.outbox (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    kind text NOT NULL,
    aggregate_id text NOT NULL,
    event_seq bigint NOT NULL,
    idempotency_key text NOT NULL,
    payload_ref text NOT NULL,
    traceparent text,
    state text DEFAULT 'pending'::text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    published_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT outbox_event_seq_check CHECK ((event_seq >= 0)),
    CONSTRAINT outbox_kind_check CHECK ((kind = ANY (ARRAY['audit'::text, 'tenant-control'::text, 'config-invalidation'::text, 'dispatch'::text, 'reply'::text, 'wakeup'::text, 'execution-control'::text]))),
    CONSTRAINT outbox_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'claimed'::text, 'retry_wait'::text, 'published'::text, 'dead_letter'::text]))),
    CONSTRAINT outbox_version_check CHECK ((version >= 0))
);
ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_pkey PRIMARY KEY (tenant_id, outbox_id);
ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_tenant_id_kind_idempotency_key_key UNIQUE (tenant_id, kind, idempotency_key);
CREATE INDEX outbox_claim_idx ON public.outbox USING btree (kind, state, next_attempt_at, created_at);
```

### delivery_ledger

```sql
CREATE TABLE public.delivery_ledger (
    tenant_id text NOT NULL,
    delivery_key text NOT NULL,
    segment_no integer NOT NULL,
    provider_message_id text,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    renderer_version text DEFAULT 'legacy-v1'::text NOT NULL,
    format_version text DEFAULT 'legacy-v1'::text NOT NULL,
    content_digest text DEFAULT repeat('0'::text, 64) NOT NULL,
    segment_count integer DEFAULT 1 NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    not_before timestamp with time zone DEFAULT now() NOT NULL,
    last_error_class text,
    client_request_id text NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    reconcile_attempt integer DEFAULT 0 NOT NULL,
    CONSTRAINT delivery_ledger_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT delivery_ledger_claim_check CHECK ((((state = 'sending'::text) AND (claim_owner IS NOT NULL) AND (length(btrim(claim_owner)) > 0) AND (claim_until IS NOT NULL)) OR ((state <> 'sending'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL)))),
    CONSTRAINT delivery_ledger_client_request_id_check CHECK ((length(btrim(client_request_id)) > 0)),
    CONSTRAINT delivery_ledger_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT delivery_ledger_format_version_check CHECK ((length(btrim(format_version)) > 0)),
    CONSTRAINT delivery_ledger_reconcile_attempt_check CHECK ((reconcile_attempt >= 0)),
    CONSTRAINT delivery_ledger_renderer_version_check CHECK ((length(btrim(renderer_version)) > 0)),
    CONSTRAINT delivery_ledger_segment_count_check CHECK (((segment_count >= 1) AND (segment_no < segment_count))),
    CONSTRAINT delivery_ledger_segment_no_check CHECK ((segment_no >= 0)),
    CONSTRAINT delivery_ledger_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'sending'::text, 'sent'::text, 'ambiguous'::text, 'retry_wait'::text, 'failed'::text]))),
    CONSTRAINT delivery_ledger_version_check CHECK ((version >= 1))
);
ALTER TABLE ONLY public.delivery_ledger
    ADD CONSTRAINT delivery_ledger_pkey PRIMARY KEY (tenant_id, delivery_key, segment_no);
CREATE INDEX delivery_ledger_claim_expiry_idx ON public.delivery_ledger USING btree (claim_until) WHERE (state = 'sending'::text);
CREATE INDEX delivery_ledger_retry_idx ON public.delivery_ledger USING btree (state, not_before, updated_at) WHERE (state = ANY (ARRAY['pending'::text, 'retry_wait'::text, 'ambiguous'::text]));
```

### delivery_resolution

```sql
CREATE TABLE public.delivery_resolution (
    tenant_id text NOT NULL,
    delivery_key text NOT NULL,
    segment_no integer NOT NULL CHECK (segment_no >= 0),
    expected_version bigint NOT NULL CHECK (expected_version >= 1),
    actor text NOT NULL CHECK (length(btrim(actor)) BETWEEN 1 AND 256),
    evidence_ref text NOT NULL CHECK (length(btrim(evidence_ref)) BETWEEN 1 AND 1024),
    decision text NOT NULL CHECK (decision IN ('delivered', 'not_delivered', 'retry_not_delivered')),
    provider_message_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,delivery_key,segment_no,expected_version),
    FOREIGN KEY (tenant_id,delivery_key,segment_no)
        REFERENCES public.delivery_ledger(tenant_id,delivery_key,segment_no),
    CHECK ((decision='delivered' AND provider_message_id IS NOT NULL AND length(btrim(provider_message_id)) > 0)
        OR (decision IN ('not_delivered', 'retry_not_delivered') AND provider_message_id IS NULL))
);
```

### prepared_payload

```sql
CREATE TABLE public.prepared_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    payload_ref text NOT NULL,
    source_payload_ref text NOT NULL,
    payload_ciphertext bytea NOT NULL,
    payload_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    artifact_retention_seconds bigint DEFAULT 0 NOT NULL,
    CONSTRAINT prepared_payload_artifact_retention_seconds_check CHECK ((artifact_retention_seconds >= 0)),
    CONSTRAINT prepared_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT prepared_payload_key_version_check CHECK ((key_version > 0))
);
ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_pkey PRIMARY KEY (tenant_id, request_id);
ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_payload_ref_key UNIQUE (tenant_id, payload_ref);
ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);
ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);
```

### tool_execution

```sql
CREATE TABLE public.tool_execution (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    tool_call_id text NOT NULL,
    tool_id text NOT NULL,
    tool_version bigint NOT NULL,
    args_digest text NOT NULL,
    recovery_policy text NOT NULL,
    idempotency_key text NOT NULL,
    state text NOT NULL,
    attempt integer NOT NULL DEFAULT 0,
    fence bigint NOT NULL DEFAULT 0,
    lease_owner text,
    lease_until timestamp with time zone,
    external_handle text,
    result_ref text,
    last_error text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, request_id, tool_call_id),
    CONSTRAINT tool_execution_policy_check CHECK (recovery_policy IN ('replay_safe','idempotent_key','queryable','manual')),
    CONSTRAINT tool_execution_state_check CHECK (state IN ('pending','running','succeeded','failed','effect_unknown')),
    CONSTRAINT tool_execution_attempt_check CHECK (attempt >= 0),
    CONSTRAINT tool_execution_fence_check CHECK (fence >= 0),
    CONSTRAINT tool_execution_lease_check CHECK ((state = 'running' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR (state <> 'running')),
    CONSTRAINT tool_execution_idempotency_key_check CHECK (length(btrim(idempotency_key)) > 0)
);
CREATE INDEX tool_execution_reclaim_idx ON public.tool_execution (lease_until) WHERE state = 'running';
CREATE UNIQUE INDEX tool_execution_idempotency_idx ON public.tool_execution (tenant_id, tool_id, tool_version, idempotency_key);
```

### tool_execution_result

```sql
CREATE TABLE public.tool_execution_result (
 tenant_id text NOT NULL, request_id text NOT NULL, tool_call_id text NOT NULL,
 result_ref text NOT NULL, content_digest text NOT NULL CHECK(content_digest ~ '^[0-9a-f]{64}$'),
 ciphertext bytea NOT NULL, nonce bytea NOT NULL CHECK(octet_length(nonce)=12), key_version bigint NOT NULL CHECK(key_version>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,request_id,tool_call_id),
 FOREIGN KEY(tenant_id,request_id,tool_call_id) REFERENCES public.tool_execution(tenant_id,request_id,tool_call_id)
);
```

### tool_attempt

```sql
CREATE TABLE public.tool_attempt (
    tenant_id text NOT NULL,
    grant_id text NOT NULL,
    request_id text NOT NULL,
    tool_call_id text NOT NULL,
    state text NOT NULL,
    result_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tool_attempt_state_check CHECK ((state = ANY (ARRAY['effect_unknown'::text, 'succeeded'::text, 'failed'::text])))
);
ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_pkey PRIMARY KEY (tenant_id, grant_id);
ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_tenant_id_request_id_tool_call_id_key UNIQUE (tenant_id, request_id, tool_call_id);
ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_tenant_id_grant_id_fkey FOREIGN KEY (tenant_id, grant_id) REFERENCES public.confirmation_grant(tenant_id, grant_id);
```

### tool_result_payload

```sql
CREATE TABLE public.tool_result_payload (
    tenant_id text NOT NULL,
    grant_id text NOT NULL,
    request_id text NOT NULL,
    result_ref text NOT NULL,
    result_ciphertext bytea NOT NULL,
    result_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tool_result_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT tool_result_payload_key_version_check CHECK ((key_version >= 1))
);
ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_pkey PRIMARY KEY (tenant_id, grant_id);
ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_result_ref_key UNIQUE (tenant_id, result_ref);
ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_grant_id_fkey FOREIGN KEY (tenant_id, grant_id) REFERENCES public.tool_attempt(tenant_id, grant_id);
ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);
```

### local_note

```sql
CREATE TABLE public.local_note (
 tenant_id text NOT NULL, business_key text NOT NULL, request_id text NOT NULL,
 note_id text NOT NULL, args_digest text NOT NULL CHECK(args_digest ~ '^[0-9a-f]{64}$'),
 ciphertext bytea NOT NULL, nonce bytea NOT NULL CHECK(octet_length(nonce)=12), key_version bigint NOT NULL CHECK(key_version>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,business_key), UNIQUE(tenant_id,note_id)
);
```


## 三个增量迁移的发布顺序

000002 建立工具执行租约；000003 建立投递异常处置审计；000004 增加执行尝试、模型 HTTP 计数、加密工具结果和笔记业务效果。当前写入方依赖全部四个迁移，不可在回滚代码前删除已被数据引用的列/表/密钥版本。

## 核心接口与工具算法（当前原文）


```go
package tool

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// RecoveryPolicy declares what a new owner may safely do after a tool lease
// expires while the previous owner was executing an external operation.
type RecoveryPolicy string

const (
	RecoveryReplaySafe    RecoveryPolicy = "replay_safe"
	RecoveryIdempotentKey RecoveryPolicy = "idempotent_key"
	RecoveryQueryable     RecoveryPolicy = "queryable"
	RecoveryManual        RecoveryPolicy = "manual"
)

// RecoveryDescriptor is implemented by tools with an explicit takeover
// contract. The key is stable for one tenant/request/tool-call/arguments
// tuple and is safe to pass to an external provider as its idempotency key.
type RecoveryDescriptor interface {
	RecoveryPolicy() RecoveryPolicy
	RecoveryKey(context.Context) (string, error)
}

// RecoveryQuerier lets a queryable tool resolve a previously accepted
// external operation by its stable idempotency key without replaying it.
// completed=false must establish that the operation was NOT accepted and may
// safely be started. Pending/unknown operations must return an error, not false.
type RecoveryQuerier interface {
	RecoveryDescriptor
	QueryRecovery(context.Context, string) (result any, completed bool, err error)
}

// RecoveryContext exposes only the durable execution identity to a tool.
func RecoveryContext(ctx context.Context) (runtime.ExecutionContext, bool) {
	return runtime.ExecutionContextFrom(ctx)
}
```


```go
package execution

import (
	"context"
	"time"
)

type State string

const (
	Pending       State = "pending"
	Running       State = "running"
	Succeeded     State = "succeeded"
	Failed        State = "failed"
	EffectUnknown State = "effect_unknown"
)

type Key struct{ TenantID, RequestID, ToolCallID string }
type Record struct {
	Key
	ToolID                               string
	ToolVersion                          int64
	ArgsDigest                           string
	Policy                               string
	IdempotencyKey                       string
	State                                State
	Attempt                              int
	Fence                                uint64
	LeaseOwner                           string
	LeaseUntil                           time.Time
	ExternalHandle, ResultRef, LastError string
}
type Claim struct {
	Owner string
	TTL   time.Duration
}
type Store interface {
	Claim(context.Context, Record, Claim) (Record, bool, error)
	Renew(context.Context, Record, time.Duration) (Record, error)
	Finish(context.Context, Record, State, string, string) (Record, error)
	Get(context.Context, Key) (Record, error)
}

// DurableResults atomically fences completion and persists encrypted output.
type DurableResults interface {
	Complete(context.Context, Record, []byte, int64) (Record, error)
	LoadResult(context.Context, Key) ([]byte, error)
}
```


```go
package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

type Store struct {
	db   *sql.DB
	keys messaging.PayloadKeyResolver
}

func New(db *sql.DB) *Store { return &Store{db: db} }
func NewWithPayloadKeys(db *sql.DB, keys messaging.PayloadKeyResolver) *Store {
	return &Store{db: db, keys: keys}
}
func (s *Store) Get(ctx context.Context, k execution.Key) (r execution.Record, e error) {
	if s == nil || s.db == nil {
		return r, runtime.ErrCapabilityUnsupported
	}
	var fence int64
	e = s.db.QueryRowContext(ctx, `SELECT tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,COALESCE(lease_owner,''),COALESCE(lease_until,'epoch'),COALESCE(external_handle,''),COALESCE(result_ref,''),COALESCE(last_error,'') FROM tool_execution WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3`, k.TenantID, k.RequestID, k.ToolCallID).Scan(&r.ToolID, &r.ToolVersion, &r.ArgsDigest, &r.Policy, &r.IdempotencyKey, &r.State, &r.Attempt, &fence, &r.LeaseOwner, &r.LeaseUntil, &r.ExternalHandle, &r.ResultRef, &r.LastError)
	r.Key = k
	r.Fence = uint64(fence)
	if e == sql.ErrNoRows {
		e = runtime.ErrNotFound
	}
	return
}

func (s *Store) Claim(ctx context.Context, in execution.Record, claim execution.Claim) (execution.Record, bool, error) {
	if s == nil || s.db == nil || in.TenantID == "" || in.RequestID == "" || in.ToolCallID == "" || in.ToolID == "" || in.ToolVersion < 1 || in.ArgsDigest == "" || in.IdempotencyKey == "" || claim.Owner == "" || claim.TTL <= 0 {
		return execution.Record{}, false, runtime.ErrInvariantViolation
	}
	var r execution.Record
	var fence int64
	var lease sql.NullTime
	err := s.db.QueryRowContext(ctx, `INSERT INTO tool_execution(tenant_id,request_id,tool_call_id,tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,lease_owner,lease_until)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,'running',1,1,$9,now()+$10::interval)
ON CONFLICT (tenant_id,request_id,tool_call_id) DO UPDATE SET state='running',attempt=tool_execution.attempt+1,fence=tool_execution.fence+1,lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until,updated_at=now()
WHERE (tool_execution.state IN ('pending','effect_unknown') OR (tool_execution.state='running' AND tool_execution.lease_until<now()))
AND tool_execution.tool_id=EXCLUDED.tool_id AND tool_execution.tool_version=EXCLUDED.tool_version
AND tool_execution.args_digest=EXCLUDED.args_digest AND tool_execution.recovery_policy=EXCLUDED.recovery_policy
AND tool_execution.idempotency_key=EXCLUDED.idempotency_key
RETURNING tenant_id,request_id,tool_call_id,tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,lease_owner,lease_until,COALESCE(external_handle,''),COALESCE(result_ref,''),COALESCE(last_error,'')`,
		in.TenantID, in.RequestID, in.ToolCallID, in.ToolID, in.ToolVersion, in.ArgsDigest, in.Policy, in.IdempotencyKey, claim.Owner, claim.TTL.String()).Scan(&r.TenantID, &r.RequestID, &r.ToolCallID, &r.ToolID, &r.ToolVersion, &r.ArgsDigest, &r.Policy, &r.IdempotencyKey, &r.State, &r.Attempt, &fence, &r.LeaseOwner, &lease, &r.ExternalHandle, &r.ResultRef, &r.LastError)
	if err == sql.ErrNoRows {
		return in, false, nil
	}
	if err != nil {
		return execution.Record{}, false, err
	}
	r.Fence = uint64(fence)
	if lease.Valid {
		r.LeaseUntil = lease.Time
	}
	return r, true, nil
}

func (s *Store) Renew(ctx context.Context, in execution.Record, ttl time.Duration) (execution.Record, error) {
	if s == nil || s.db == nil || in.LeaseOwner == "" || in.Fence == 0 || ttl <= 0 {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tool_execution SET lease_until=now()+$1::interval,updated_at=now() WHERE tenant_id=$2 AND request_id=$3 AND tool_call_id=$4 AND state='running' AND lease_owner=$5 AND fence=$6 AND lease_until>now()`, ttl.String(), in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence))
	if err != nil {
		return execution.Record{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return execution.Record{}, runtime.ErrVersionConflict
	}
	return s.Get(ctx, in.Key)
}

func (s *Store) Finish(ctx context.Context, in execution.Record, state execution.State, resultRef, lastError string) (execution.Record, error) {
	if state != execution.Succeeded && state != execution.Failed && state != execution.EffectUnknown {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	if in.LeaseOwner == "" || in.Fence == 0 {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tool_execution SET state=$1,result_ref=NULLIF($2,''),last_error=NULLIF($3,''),lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE tenant_id=$4 AND request_id=$5 AND tool_call_id=$6 AND state='running' AND lease_owner=$7 AND fence=$8 AND lease_until>now()`, state, resultRef, lastError, in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence))
	if err != nil {
		return execution.Record{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return execution.Record{}, runtime.ErrVersionConflict
	}
	return s.Get(ctx, in.Key)
}
```


```go
package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

func resultAAD(key execution.Key, digest string, version int64) []byte {
	data, _ := json.Marshal([]any{"tool-execution-result-v1", key.TenantID, key.RequestID, key.ToolCallID, digest, version})
	return data
}
func (s *Store) aead(ctx context.Context, tenant string, version int64) (cipher.AEAD, error) {
	if s.keys == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	key, err := s.keys.ResolvePayloadKey(ctx, tenant, version)
	if err != nil {
		return nil, err
	}
	defer clear(key.Bytes)
	if key.Version != version || len(key.Bytes) != 32 {
		return nil, runtime.ErrVersionMismatch
	}
	block, err := aes.NewCipher(key.Bytes)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (s *Store) Complete(ctx context.Context, in execution.Record, content []byte, version int64) (execution.Record, error) {
	if len(content) == 0 || version < 1 || in.Fence == 0 || in.LeaseOwner == "" {
		return in, runtime.ErrInvalidEnvelope
	}
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	ref := "tool-result://sha256/" + digest
	aead, err := s.aead(ctx, in.TenantID, version)
	if err != nil {
		return in, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return in, err
	}
	encrypted := aead.Seal(nil, nonce, content, resultAAD(in.Key, digest, version))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return in, err
	}
	defer tx.Rollback()
	var fence int64
	err = tx.QueryRowContext(ctx, `UPDATE tool_execution SET state='succeeded',result_ref=$6,last_error=NULL,lease_owner=NULL,lease_until=NULL,updated_at=now()
WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3 AND state='running' AND lease_owner=$4 AND fence=$5 AND lease_until>now() RETURNING fence`, in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence), ref).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return in, runtime.ErrLeaseLost
	}
	if err != nil {
		return in, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tool_execution_result(tenant_id,request_id,tool_call_id,result_ref,content_digest,ciphertext,nonce,key_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, in.TenantID, in.RequestID, in.ToolCallID, ref, digest, encrypted, nonce, version)
	if err != nil {
		return in, err
	}
	if err = tx.Commit(); err != nil {
		return in, err
	}
	in.State, in.ResultRef, in.LeaseOwner = execution.Succeeded, ref, ""
	return in, nil
}
func (s *Store) LoadResult(ctx context.Context, key execution.Key) ([]byte, error) {
	var digest string
	var encrypted, nonce []byte
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT r.content_digest,r.ciphertext,r.nonce,r.key_version FROM tool_execution_result r JOIN tool_execution e USING(tenant_id,request_id,tool_call_id) WHERE r.tenant_id=$1 AND r.request_id=$2 AND r.tool_call_id=$3 AND e.state='succeeded' AND e.result_ref=r.result_ref`, key.TenantID, key.RequestID, key.ToolCallID).Scan(&digest, &encrypted, &nonce, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, runtime.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	aead, err := s.aead(ctx, key.TenantID, version)
	if err != nil {
		return nil, err
	}
	if len(nonce) != aead.NonceSize() {
		return nil, runtime.ErrInvariantViolation
	}
	content, err := aead.Open(nil, nonce, encrypted, resultAAD(key, digest, version))
	if err != nil {
		return nil, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != digest {
		clear(content)
		return nil, runtime.ErrVersionMismatch
	}
	return content, nil
}
```


## Worker 尝试与模型 HTTP 计数（当前原文）


```go
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func (s *TaskStore) BeginExecutionAttempt(ctx context.Context, key gateway.ExecutionKey, worker string, fence uint64) (gateway.ExecutionAttempt, error) {
	a := gateway.ExecutionAttempt{ExecutionKey: key, WorkerID: worker, Fence: fence}
	if key.TenantID == "" || key.RequestID == "" || worker == "" || fence == 0 || fence > math.MaxInt64 {
		return a, runtime.ErrInvalidEnvelope
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	var request string
	if err = tx.QueryRowContext(ctx, `SELECT request_id FROM execution_record WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, key.TenantID, key.RequestID).Scan(&request); err != nil {
		return a, err
	}
	var maximum int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(fence),0) FROM execution_attempt WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, key.RequestID).Scan(&maximum); err != nil {
		return a, err
	}
	if uint64(maximum) > fence {
		return a, runtime.ErrStaleFence
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_attempt SET state='superseded',finished_at=now() WHERE tenant_id=$1 AND request_id=$2 AND state='running'`, key.TenantID, key.RequestID); err != nil {
		return a, err
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO execution_attempt(tenant_id,request_id,attempt_no,worker_id,fence,state)
SELECT $1,$2,COALESCE(max(attempt_no),0)+1,$3,$4,'running' FROM execution_attempt WHERE tenant_id=$1 AND request_id=$2 RETURNING attempt_no`, key.TenantID, key.RequestID, worker, int64(fence)).Scan(&a.Number)
	if err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (s *TaskStore) FinishExecutionAttempt(ctx context.Context, a gateway.ExecutionAttempt, completed bool) error {
	state := "retryable"
	if completed {
		state = "completed"
	}
	var version int64
	err := s.db.QueryRowContext(ctx, `UPDATE execution_attempt SET state=$6,finished_at=now() WHERE tenant_id=$1 AND request_id=$2 AND attempt_no=$3 AND worker_id=$4 AND fence=$5 AND state='running' RETURNING attempt_no`, a.TenantID, a.RequestID, a.Number, a.WorkerID, int64(a.Fence), state).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrVersionConflict
	}
	return err
}

func (s *TaskStore) RecordModelRequest(ctx context.Context, key gateway.ExecutionKey) error {
	if key.TenantID == "" || key.RequestID == "" {
		return runtime.ErrTenantScope
	}
	result, err := s.db.ExecContext(ctx, `UPDATE execution_record SET model_request_count=model_request_count+1 WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, key.RequestID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return runtime.ErrNotFound
	}
	return nil
}
```


## 提交、队列与 Ledger 契约

commit_turn 和 Redis完整实现见 [会话参考实现](../conversation-continuity/REFERENCE-IMPLEMENTATION.md)。Worker ACK 的语义是本 delivery 已有 durable disposition：terminal、停放或阻塞等后续责任已持久化；不能只用 ACK 推断用户已收到回复。Relay 先 Publish 再 MarkPublished，故可重复 event。Delivery 服务固定计划、claim、续期、发送、确认或核对；其完整生产算法如下。


```go
// Package delivery owns provider-neutral reliable reply delivery after the
// account queue. Provider adapters are called only after a Delivery Ledger
// segment claim succeeds.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/inflight"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type AdapterResolver interface {
	ResolveAdapter(context.Context, string, string) (channel.Adapter, error)
}

type VersionedAdapterResolver interface {
	ResolveVersionedAdapter(context.Context, string, string, int64) (channel.Adapter, error)
}

// Compatibility aliases keep delivery callers source-compatible while the
// error taxonomy itself lives in the provider-neutral Adapter contract.
type AmbiguousError = channel.AmbiguousDeliveryError
type RetryAfterError = channel.RetryableDeliveryError
type PermanentError = channel.PermanentDeliveryError

// DeferredError keeps the account-queue entry pending while another delivery
// attempt is active or its durable retry deadline has not arrived.
type DeferredError struct{ NotBefore time.Time }

func (e DeferredError) Error() string { return "reply delivery is deferred" }

// TerminalError means the failure was durably recorded as DeliveryFailed and
// the account-queue entry can be ACKed after the error is reported.
type TerminalError struct{ Err error }

func (e TerminalError) Error() string {
	if e.Err == nil {
		return "reply delivery permanently failed"
	}
	return e.Err.Error()
}
func (e TerminalError) Unwrap() error { return e.Err }

type Service struct {
	Results              messaging.ResultStore
	Ledger               messaging.DeliveryLedger
	Adapters             AdapterResolver
	Owner                string
	ClaimTTL             time.Duration
	ClaimRenewInterval   time.Duration
	RendererVersion      string
	FormatVersion        string
	DefaultRetryDelay    time.Duration
	MaxRetryDelay        time.Duration
	MaxAttempts          int
	MaxReconcileAttempts int
	Barrier              inflight.Barrier
}

func (s Service) Deliver(ctx context.Context, event channel.ReplyEvent) error {
	if s.Results == nil || s.Ledger == nil || s.Adapters == nil || event.SchemaVersion != 1 ||
		s.Owner == "" || event.TenantID == "" || event.RequestID == "" || event.ChannelBindingID == "" || event.ConfigVersion < 1 || event.DeliveryKey == "" || event.ContentRef == "" ||
		event.Target.Channel == "" || event.Target.ExternalAccountID == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := messaging.ResolveReplyContent(ctx, s.Results, event.TenantID, event.RequestID, event.ContentRef)
	if err != nil {
		return err
	}
	if result.ResultRef != event.ContentRef {
		return runtime.ErrVersionMismatch
	}
	adapter, err := s.resolveAdapter(ctx, event)
	if err != nil {
		return err
	}
	if adapter == nil || adapter.ID() != event.Target.Channel {
		return runtime.ErrTenantScope
	}
	contentType, err := messaging.NormalizeContentType(result.ContentType)
	if err != nil {
		return err
	}
	segments := [][]byte{append([]byte(nil), result.Content...)}
	if contentType == messaging.ContentTypeText {
		segments = splitText(result.Content, maxTextBytes(adapter))
	}
	plan := messaging.DeliveryPlan{RendererVersion: s.rendererVersion(), FormatVersion: s.formatVersion(contentType, len(segments)), ContentDigest: result.ContentDigest, SegmentCount: len(segments)}
	for segmentNo, content := range segments {
		if err := s.deliverSegment(ctx, event, adapter, plan, segmentNo, content, contentType); err != nil {
			return err
		}
	}
	return nil
}

func (s Service) deliverSegment(ctx context.Context, event channel.ReplyEvent, adapter channel.Adapter, plan messaging.DeliveryPlan, segmentNo int, content []byte, contentType string) error {
	ctx = inflight.WithScope(ctx, event.TenantID, event.RequestID, "")
	key := messaging.DeliveryKey{TenantID: event.TenantID, DeliveryKey: event.DeliveryKey, SegmentNo: segmentNo}
	claimTTL := s.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}
	record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
	if err != nil {
		return err
	}
	if !acquired {
		switch record.State {
		case messaging.DeliverySent, messaging.DeliveryFailed:
			return nil
		case messaging.DeliveryAmbiguous:
			return s.reconcile(ctx, event, record)
		case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
			return DeferredError{NotBefore: record.NotBefore}
		default:
			return runtime.ErrInvariantViolation
		}
	}
	if s.Barrier != nil {
		if err := s.Barrier.Wait(ctx, inflight.PointP4BeforeProviderDelivery); err != nil {
			return err
		}
	}
	contentDigest := plan.ContentDigest
	if plan.SegmentCount > 1 {
		sum := sha256.Sum256(content)
		contentDigest = hex.EncodeToString(sum[:])
	}
	record, resultDelivery, deliverErr := s.deliverWithClaimRenewal(ctx, adapter, channel.DeliveryRequest{
		Event: event, ClientRequestID: record.ClientRequestID, Target: event.Target,
		Content: append([]byte(nil), content...), ContentDigest: contentDigest, ContentType: contentType,
	}, record, claimTTL)
	if deliverErr != nil {
		var ambiguous AmbiguousError
		if errors.As(deliverErr, &ambiguous) {
			record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "response_lost"
			_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
			return errors.Join(deliverErr, finishErr)
		}
		var permanent PermanentError
		if errors.As(deliverErr, &permanent) {
			return s.finishFailed(ctx, record, deliverErr, permanent.Class, false)
		}
		var retryAfter RetryAfterError
		if errors.As(deliverErr, &retryAfter) {
			return s.finishRetry(ctx, record, deliverErr, retryAfter.RetryAfter)
		}
		return s.finishRetry(ctx, record, deliverErr, 0)
	}
	if !resultDelivery.Delivered {
		return s.finishRetry(ctx, record, runtime.ErrBackendUnavailable, 0)
	}
	if resultDelivery.ProviderMessageID == "" {
		record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "missing_provider_message_id"
		_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
		return errors.Join(AmbiguousError{Err: runtime.ErrInvariantViolation}, finishErr)
	}
	record.State = messaging.DeliverySent
	record.ProviderMessageID = resultDelivery.ProviderMessageID
	record.LastErrorClass = ""
	_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	return err
}

func (s Service) rendererVersion() string {
	if s.RendererVersion != "" {
		return s.RendererVersion
	}
	return "terminal-text-v1"
}

func (s Service) formatVersion(contentType string, segmentCount int) string {
	if s.FormatVersion != "" {
		return s.FormatVersion
	}
	if contentType == messaging.ContentTypeCard {
		return "card-v1"
	}
	if strings.HasPrefix(contentType, "image/") {
		return "image-v1"
	}
	if segmentCount > 1 {
		return "text-segment-v1"
	}
	return "text-v1"
}

func maxTextBytes(adapter channel.Adapter) int {
	if limit, ok := adapter.(channel.TextSegmentLimit); ok && limit.MaxTextBytes() > 0 {
		return limit.MaxTextBytes()
	}
	return 0
}

// splitText preserves every input byte while ensuring that a segment never
// ends in the middle of a UTF-8 rune. Invalid UTF-8 is passed through as one
// segment so the provider adapter can retain its existing permanent-error
// classification instead of silently changing the response content.
func splitText(content []byte, maxBytes int) [][]byte {
	if maxBytes <= 0 || len(content) <= maxBytes || !utf8.Valid(content) {
		return [][]byte{append([]byte(nil), content...)}
	}
	segments := make([][]byte, 0, (len(content)+maxBytes-1)/maxBytes)
	for start := 0; start < len(content); {
		end := start + maxBytes
		if end >= len(content) {
			segments = append(segments, append([]byte(nil), content[start:]...))
			break
		}
		for end > start && !utf8.Valid(content[start:end]) {
			end--
		}
		// maxBytes can be smaller than the first rune only for a malformed
		// adapter declaration. Preserve the original response in that case.
		if end == start {
			return [][]byte{append([]byte(nil), content...)}
		}
		segments = append(segments, append([]byte(nil), content[start:end]...))
		start = end
	}
	return segments
}

func (s Service) reconcile(ctx context.Context, event channel.ReplyEvent, record messaging.DeliveryRecord) error {
	if record.NotBefore.After(time.Now()) {
		return DeferredError{NotBefore: record.NotBefore}
	}
	adapter, err := s.resolveAdapter(ctx, event)
	if err != nil {
		return s.deferReconciliation(ctx, record, err)
	}
	reconciler, ok := adapter.(channel.DeliveryReconciler)
	if !ok {
		return s.deferReconciliation(ctx, record, runtime.ErrCapabilityUnsupported)
	}
	result, err := reconciler.ReconcileDelivery(ctx, channel.ReconciliationRequest{Event: event, ClientRequestID: record.ClientRequestID})
	if err != nil {
		return s.deferReconciliation(ctx, record, err)
	}
	switch result.Status {
	case channel.ReconciliationDelivered:
		if result.ProviderMessageID == "" {
			return s.deferReconciliation(ctx, record, runtime.ErrInvariantViolation)
		}
		record.State, record.ProviderMessageID, record.LastErrorClass = messaging.DeliverySent, result.ProviderMessageID, ""
		_, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version)
		return err
	case channel.ReconciliationNotDelivered:
		record.State, record.NotBefore, record.LastErrorClass = messaging.DeliveryRetryWait, time.Now().UTC(), "reconciled_not_delivered"
		if _, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version); err != nil {
			return err
		}
		return DeferredError{NotBefore: record.NotBefore}
	case channel.ReconciliationUnknown:
		return s.deferReconciliation(ctx, record, runtime.ErrCommitConflict)
	default:
		return s.deferReconciliation(ctx, record, runtime.ErrInvariantViolation)
	}
}

func (s Service) resolveAdapter(ctx context.Context, event channel.ReplyEvent) (channel.Adapter, error) {
	if resolver, ok := s.Adapters.(VersionedAdapterResolver); ok {
		return resolver.ResolveVersionedAdapter(ctx, event.TenantID, event.ChannelBindingID, event.ConfigVersion)
	}
	return s.Adapters.ResolveAdapter(ctx, event.TenantID, event.ChannelBindingID)
}

func (s Service) finishRetry(ctx context.Context, record messaging.DeliveryRecord, cause error, delay time.Duration) error {
	if record.Attempt >= s.maxAttempts() {
		return s.finishFailed(ctx, record, cause, "retry_exhausted", false)
	}
	delay = s.backoff(record.Attempt, delay)
	record.State = messaging.DeliveryRetryWait
	record.NotBefore = time.Now().Add(delay)
	record.LastErrorClass = "retryable"
	_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
	return errors.Join(cause, finishErr)
}

type renewalResult struct {
	record messaging.DeliveryRecord
	err    error
}

func (s Service) deliverWithClaimRenewal(ctx context.Context, adapter channel.Adapter, request channel.DeliveryRequest, record messaging.DeliveryRecord, claimTTL time.Duration) (messaging.DeliveryRecord, channel.DeliveryResult, error) {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := s.ClaimRenewInterval
	if interval <= 0 || interval >= claimTTL {
		interval = claimTTL / 3
	}
	if interval <= 0 {
		interval = time.Nanosecond
	}
	stop := make(chan struct{})
	done := make(chan renewalResult, 1)
	go func() {
		latest := record
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- renewalResult{record: latest}
				return
			case <-callCtx.Done():
				done <- renewalResult{record: latest, err: callCtx.Err()}
				return
			case <-ticker.C:
				renewed, err := s.Ledger.RenewDeliveryClaim(callCtx, latest, claimTTL)
				if err != nil {
					cancel()
					done <- renewalResult{record: latest, err: err}
					return
				}
				latest = renewed
			}
		}
	}()
	result, deliverErr := adapter.Deliver(callCtx, request)
	if deliverErr == nil && result.Delivered && result.ProviderMessageID != "" && s.Barrier != nil {
		// Keep renewing the sending claim while paused. The accepted-send
		// window must not migrate to a peer merely because observation is slow.
		if pauseErr := s.Barrier.Wait(inflight.WithScope(callCtx, request.Event.TenantID, request.Event.RequestID, result.ProviderMessageID), inflight.PointP4AfterProviderAccepted); pauseErr != nil {
			deliverErr = AmbiguousError{Err: pauseErr}
		}
	}
	close(stop)
	renewal := <-done
	if renewal.err != nil {
		err := errors.Join(deliverErr, renewal.err)
		if result.Delivered {
			err = AmbiguousError{Err: err}
		}
		return renewal.record, result, err
	}
	return renewal.record, result, deliverErr
}

func (s Service) deferReconciliation(ctx context.Context, record messaging.DeliveryRecord, cause error) error {
	nextAttempt := record.ReconcileAttempt + 1
	if nextAttempt >= s.maxReconcileAttempts() {
		return s.finishFailed(ctx, record, cause, "reconcile_exhausted", true)
	}
	record.ReconcileAttempt = nextAttempt
	record.NotBefore = time.Now().Add(s.backoff(nextAttempt, 0))
	record.LastErrorClass = "reconcile_retryable"
	updated, err := s.Ledger.DeferDeliveryReconciliation(ctx, record, record.Version)
	if err != nil {
		return errors.Join(cause, err)
	}
	return DeferredError{NotBefore: updated.NotBefore}
}

func (s Service) finishFailed(ctx context.Context, record messaging.DeliveryRecord, cause error, class string, reconcile bool) error {
	record.State = messaging.DeliveryFailed
	record.LastErrorClass = normalizeErrorClass(class)
	var err error
	if reconcile {
		_, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version)
	} else {
		_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	}
	if err != nil {
		return errors.Join(cause, err)
	}
	return TerminalError{Err: cause}
}

func (s Service) backoff(attempt int, requested time.Duration) time.Duration {
	base := s.DefaultRetryDelay
	if base <= 0 {
		base = time.Second
	}
	if requested > 0 {
		base = requested
	} else if attempt > 1 {
		shift := attempt - 1
		if shift > 20 {
			shift = 20
		}
		factor := time.Duration(1 << shift)
		maximum := s.MaxRetryDelay
		if maximum <= 0 {
			maximum = time.Minute
		}
		if base > maximum/factor {
			return maximum
		}
		base *= factor
	}
	maximum := s.MaxRetryDelay
	if maximum <= 0 {
		maximum = time.Minute
	}
	if base > maximum {
		return maximum
	}
	return base
}

func (s Service) maxAttempts() int {
	if s.MaxAttempts > 0 {
		return s.MaxAttempts
	}
	return 8
}

func (s Service) maxReconcileAttempts() int {
	if s.MaxReconcileAttempts > 0 {
		return s.MaxReconcileAttempts
	}
	return 8
}

func normalizeErrorClass(class string) string {
	class = strings.TrimSpace(class)
	if class == "" {
		return "permanent"
	}
	for _, char := range class {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return "permanent"
	}
	return class
}
```


## 配置契约

| 参数 | 当前值/要求 |
|---|---|
| TRPC_POSTGRES_DSN / TRPC_REDIS_ADDRESS | 共享数据库与 Redis，两个实例须相同。 |
| TRPC_REDIS_ENVIRONMENT | 默认 local-runtime；同一部署协调命名空间相同，各环境不得误共用。 |
| TRPC_WEBUI_LOCAL_INSTANCE_ID | standalone 默认；Compose 每节点不同。 |
| TRPC_WEBUI_LOCAL_STATUS_ENABLED | true 才注册 status；owner 启动身份始终生成。 |
| TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT | 默认 /tmp/trpc-webui-skills；多实例 /var/lib/trpc-webui-local/skills。 |
| TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY | 节点 true；bootstrap false。 |
| TRPC_WEBUI_LOCAL_SECRET_ROOT | 默认 /tmp/trpc-webui-secrets，每容器私有。 |
| TRPC_RUNTIME_DRAIN_TIMEOUT | 默认30s，允许1s–5m。 |
| TRPC_RUNTIME_OPERATIONS_TOKEN | 独立管理 token，至少32字符，不复用 WebUI token；为空不注册 ops。 |
| TRPC_WECOM_LOCAL_ENABLED | 主 Bot 渠道开关。 |
| TRPC_WECOM_SECONDARY_LOCAL_ENABLED | 次 Bot 渠道开关；需要独立 Corp/Agent 组合与全套凭据。 |
| TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED | 仅隔离演练环境 true；默认不开故障控制面。 |

Bot 凭据通过 Compose secrets/wecom.env 注入，模型 key 通过 secrets/deepseek-api-key。配置名和鉴权严格以组合根为准；不要把脚本的 assumed 当成运行时配置或业务成功状态。

## 算法与启动顺序

1. 先完成迁移和共享依赖，再发布 tenant/app/Binding/config/policy 与密钥版本；一次性 bootstrap 发布不可变技能内容。
2. 两节点使用相同业务后端/协调环境，独立 instance_id、本地 /tmp 和启动身份；共享技能卷按只读加载。
3. 装配 SDK session service、工具 durable store+payload keys、preprocess/dispatch/Worker/Relay/Wakeup/Delivery；启动实际轮询，ready 之前不导入业务流量。
4. 稳定入口探测 ready 节点；两 Bot 对应不同 route key，始终使用相同公开 origin。
5. 接受输入先 durable，再异步调度；有效 owner 执行并提交；最后依据 Ledger 与下游接受结果处理发送。
6. SIGTERM 执行有界 drain；SIGKILL 依靠 lease/claim 过期和 reclaim。不要通过手工改数据让恢复看似成立。

| 参数 | 当前本地组合根装配 | 意义 |
|---|---|---|
| Worker lease / renew | 30s / 10s | 失权取消执行；lease 到期才可换 owner。 |
| Worker reclaim / limit | 5s / 100 | 扫描 pending；不代表 5 秒内必完成恢复。 |
| Relay claim / renew / poll | 30s / 10s / 100ms | 先发布，再标记 published；发布后崩溃可重发同一事件。 |
| Delivery claim / renew | 30s / 10s | 每分段发送权。 |
| Delivery reclaim / limit | 5s / 100 | 回收账号回复队列 pending。 |
| Delivery retry | 初始 1s，最大 1m，最多 8 次 | 明确可重试错误的有界退避。 |
| Delivery reconcile | 最多 8 次 | 无查询能力或核对失败最终保留 failed/reconcile_exhausted 事件。 |
| 工具 lease / renew | 默认 30s / TTL÷3 | PostgreSQL 工具 lease，与会话 lease 是两套执行权。 |
| 应用排空 | 默认 30s，可配置 1s–5m | 停止新领取，保留已接纳工作，超时再取消。 |
| Compose 停止宽限 | 默认 45s | 必须比应用排空更长并留退出余量。 |

这些值是当前 `webui-local`/`wecom-local` 装配值，不能与 Consumer 未配置时的包级兜底值混用。恢复耗时还包含排队、上游延迟、退避和采样误差，不能把 TTL 相加写成已验证的 RTO。

| 测试路径名 | 实际边界 | 证据 / 使用限制 |
|---|---|---|
| p1 | 预处理持久领取后、执行前 | 原输入/job 已持久化；不能代替所有 P1 子窗口。 |
| p2 | 模型成功响应头返回后、SDK 消费结果前 | 请求尝试与成功头状态；不证明 provider 内部仍在生成。 |
| p2-commit | 模型/工具完成、terminal 结果提交前 | 旧边界，不能代替执行中的 P2。 |
| result-before-commit | 冻结结果保存后、terminal/Outbox 提交前 | 原 result_ref/digest 不变，接管不再调用模型。 |
| p3 | terminal 已提交、reply PublishReply 前 | session_commit、result_payload、已领取 reply Outbox。 |
| p4-before-send | Ledger claim 后、provider 调用前 | 旧发送前边界，不能证明 P4 已接受。 |
| p4 | provider 成功接受且返回消息 ID 后、Ledger sent 前 | barrier 快照 Evidence；进程内快照需在 kill 前采集，非 durable receipt。 |
| tool-effect | 工具业务效果完成后、工具结果提交前 | 按业务键查外部效果，恢复不能增加效果数。 |

仅 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 注册 `/test/failover/<point>/arm|status|release`，用本地 WebUI token 鉴权。arm 可带 tenant_id/request_id 过滤；status 回显命中身份/evidence。暂停控制面是进程内测试设施，不续 lease、不修改业务状态、不替系统重投。标准接管在命中后杀 owner，不对 survivor release。

## 管理接口与不确定投递处置

设置独立的 `TRPC_RUNTIME_OPERATIONS_TOKEN`（至少 32 字符、不得复用 WebUI token）才注册管理接口。使用请求头 `X-TRPC-Local-Token`，直接访问节点；稳定 WeCom 入口不转发 `/ops/*`。响应 `Cache-Control: no-store`，查询显式带 tenant，不返回正文或密钥。

| 接口 | 参数 / 作用 |
|---|---|
| GET /ops/reliability | tenant_id；单条 SQL 一致快照，含 Inbox/执行/Outbox/Ledger/工具计数、最近执行尝试和异常/处置；明细各最多 100 条。 |
| GET /ops/session/lease | tenant_id、agent_app_id、session_id；原子读 owner/fence/剩余租期，不返回 LeaseID。 |
| POST /ops/delivery/resolve | 外部证据核实后的版本化处置；冲突 409、输入错误 400、未授权 401、成功 204。 |

处置输入为 `tenant_id, delivery_key, segment_no, expected_version, actor, evidence_ref, decision`。decision=delivered 必须带 provider_message_id；另外两种 decision 必须不带。actor 是管理调用方声明的操作人。

| decision | 状态与调度 |
|---|---|
| delivered | 分段置 sent；重调度原 reply Outbox 以继续剩余段，当前段只跳过发送。 |
| not_delivered | 分段置 failed，关闭事件，不补发。 |
| retry_not_delivered | 有未送达证据后置 retry_wait，重调度原 reply，沿用 DeliveryKey/ClientRequestID/内容与累计尝试数。 |

只能处置 ambiguous 或 failed/reconcile_exhausted。Ledger CAS、delivery_resolution 审计和必要的原 Outbox 重调度同事务提交；Outbox 正被领取时返回冲突，不能手改状态绕过。超时不能证明未送达。


## 最小复刻检查

工具结果与成功状态须原子；重试键不能随模型调用ID漂移；queryable 的 false 须证明未接受；grant 不得二次消费；冻结结果后模型调用不增加；P3不重算；P4确认前崩溃不盲补发；reconcile耗尽仍可按证据处置。外部业务效果按业务键核对，不能仅检查平台记录行数。

## 组合根完整配置校验原文

以下为当前 loadWebUILocalConfig；它给出全部本地运行时环境变量、默认值、开关、密钥路径及合法组合，不包含运行环境注入的实际密钥值。

```go
func loadWebUILocalConfig(getenv func(string) string) (webUILocalConfig, error) {
	if strings.EqualFold(strings.TrimSpace(getenv("TRPC_FEISHU_LOCAL_ENABLED")), "true") {
		return webUILocalConfig{}, errors.New("Feishu local runtime is no longer supported")
	}
	value := webUILocalConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		DrainTimeout:    30 * time.Second,
		ReadOnlySkills:  strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY")), "true"),
		OperationsToken: getenv("TRPC_RUNTIME_OPERATIONS_TOKEN"),
		RedisAddress:    strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")), ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		RedisEnvironment:             valueOr(getenv("TRPC_REDIS_ENVIRONMENT"), "local-runtime"),
		SecretRoot:                   valueOr(getenv("TRPC_WEBUI_LOCAL_SECRET_ROOT"), "/tmp/trpc-webui-secrets"),
		SkillStagingRoot:             valueOr(getenv("TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT"), "/tmp/trpc-webui-skills"),
		QdrantEndpoint:               valueOr(getenv("TRPC_WEBUI_LOCAL_QDRANT_ENDPOINT"), "http://qdrant:6333"),
		APIKeyFile:                   valueOr(getenv("TRPC_WEBUI_DEEPSEEK_KEY_FILE"), "/run/secrets/deepseek_api_key"),
		RouteKey:                     valueOr(getenv("TRPC_WEBUI_LOCAL_ROUTE_KEY"), webUILocalRouteKey),
		Token:                        valueOr(getenv("TRPC_WEBUI_LOCAL_TOKEN"), webUILocalToken),
		InstanceID:                   valueOr(getenv("TRPC_WEBUI_LOCAL_INSTANCE_ID"), "standalone"),
		ClamAVAddress:                valueOr(getenv("TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS"), "clamav:3310"),
		ExclusiveRuntime:             strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_EXCLUSIVE_RUNTIME")), "true"),
		FailoverTestEnabled:          strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED")), "true"),
		StatusEnabled:                strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_STATUS_ENABLED")), "true"),
		FeishuEnabled:                strings.EqualFold(strings.TrimSpace(getenv("TRPC_FEISHU_LOCAL_ENABLED")), "true"),
		FeishuAppID:                  strings.TrimSpace(getenv("FEISHU_APP_ID")),
		FeishuAppSecret:              strings.TrimSpace(getenv("FEISHU_APP_SECRET")),
		FeishuVerificationToken:      strings.TrimSpace(getenv("FEISHU_VERIFICATION_TOKEN")),
		FeishuEncryptKey:             strings.TrimSpace(getenv("FEISHU_ENCRYPT_KEY")),
		FeishuBotOpenID:              strings.TrimSpace(getenv("FEISHU_BOT_OPEN_ID")),
		WeComEnabled:                 strings.EqualFold(strings.TrimSpace(getenv("TRPC_WECOM_LOCAL_ENABLED")), "true"),
		WeComSecondaryEnabled:        strings.EqualFold(strings.TrimSpace(getenv("TRPC_WECOM_SECONDARY_LOCAL_ENABLED")), "true"),
		WeComCorpID:                  strings.TrimSpace(getenv("WECOM_CORP_ID")),
		WeComAppSecret:               strings.TrimSpace(getenv("WECOM_APP_SECRET")),
		WeComCallbackToken:           strings.TrimSpace(getenv("WECOM_CALLBACK_TOKEN")),
		WeComEncodingAESKey:          strings.TrimSpace(getenv("WECOM_ENCODING_AES_KEY")),
		WeComSecondaryCorpID:         strings.TrimSpace(getenv("WECOM_SECONDARY_CORP_ID")),
		WeComSecondaryAppSecret:      strings.TrimSpace(getenv("WECOM_SECONDARY_APP_SECRET")),
		WeComSecondaryCallbackToken:  strings.TrimSpace(getenv("WECOM_SECONDARY_CALLBACK_TOKEN")),
		WeComSecondaryEncodingAESKey: strings.TrimSpace(getenv("WECOM_SECONDARY_ENCODING_AES_KEY")),
	}
	if raw := strings.TrimSpace(getenv("TRPC_RUNTIME_DRAIN_TIMEOUT")); raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil || duration < time.Second || duration > 5*time.Minute {
			return webUILocalConfig{}, errors.New("invalid runtime drain timeout")
		}
		value.DrainTimeout = duration
	}
	if value.OperationsToken != "" && (len(value.OperationsToken) < 32 || strings.TrimSpace(value.OperationsToken) != value.OperationsToken || value.OperationsToken == value.Token) {
		return webUILocalConfig{}, errors.New("operations token must be independent and contain at least 32 characters")
	}
	if value.PostgresDSN == "" || value.RedisAddress == "" || strings.TrimSpace(value.Token) != value.Token || len(value.Token) < 16 ||
		strings.TrimSpace(value.RouteKey) != value.RouteKey || value.RouteKey == "" || strings.TrimSpace(value.ClamAVAddress) != value.ClamAVAddress || value.ClamAVAddress == "" || !filepath.IsAbs(value.APIKeyFile) || !filepath.IsAbs(value.SecretRoot) ||
		!filepath.IsAbs(value.SkillStagingRoot) || filepath.Clean(value.SkillStagingRoot) != value.SkillStagingRoot || value.SkillStagingRoot == value.SecretRoot || value.QdrantEndpoint != "http://qdrant:6333" {
		return webUILocalConfig{}, errors.New("required WebUI local configuration is missing or invalid")
	}
	if !validWebUILocalInstanceID(value.InstanceID) {
		return webUILocalConfig{}, errors.New("WebUI local instance ID is invalid")
	}
	if value.FeishuEnabled && (value.FeishuAppID == "" || value.FeishuAppSecret == "" || value.FeishuVerificationToken == "" || value.FeishuEncryptKey == "") {
		return webUILocalConfig{}, errors.New("Feishu local configuration is incomplete")
	}
	if value.WeComEnabled {
		agentID, agentIDErr := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_AGENT_ID")), 10, 64)
		if agentIDErr != nil || agentID <= 0 || value.WeComCorpID == "" || value.WeComAppSecret == "" || value.WeComCallbackToken == "" ||
			value.WeComEncodingAESKey == "" {
			return webUILocalConfig{}, errors.New("WeCom local configuration is incomplete")
		}
		value.WeComAgentID = agentID
	}
	secondaryConfigured := value.WeComSecondaryCorpID != "" || strings.TrimSpace(getenv("WECOM_SECONDARY_AGENT_ID")) != "" || value.WeComSecondaryAppSecret != "" ||
		value.WeComSecondaryCallbackToken != "" || value.WeComSecondaryEncodingAESKey != ""
	if !value.WeComSecondaryEnabled && secondaryConfigured {
		return webUILocalConfig{}, errors.New("secondary WeCom credentials require explicit enablement")
	}
	if value.WeComSecondaryEnabled {
		if !value.WeComEnabled {
			return webUILocalConfig{}, errors.New("secondary WeCom requires the primary WeCom local configuration")
		}
		agentID, agentIDErr := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_SECONDARY_AGENT_ID")), 10, 64)
		if agentIDErr != nil || agentID <= 0 || value.WeComSecondaryCorpID == "" || value.WeComSecondaryAppSecret == "" ||
			value.WeComSecondaryCallbackToken == "" || value.WeComSecondaryEncodingAESKey == "" ||
			(value.WeComSecondaryCorpID == value.WeComCorpID && agentID == value.WeComAgentID) {
			return webUILocalConfig{}, errors.New("secondary WeCom local configuration is incomplete or incompatible")
		}
		value.WeComSecondaryAgentID = agentID
	}
	return value, nil
}
```

## Ledger 条件更新原文

expired sending 先改为 ambiguous/owner_lost；重新发送仅能领取pending/retry_wait。FinishDelivery校验version/state/owner/client_request_id，Renew还校验租期；Finish语句本身没有lease_until谓词，不能把它描述成包含该检查。工具Complete另有有效租期条件，两者不可混淆。

```go
func (s *Store) ClaimDelivery(ctx context.Context, key messaging.DeliveryKey, plan messaging.DeliveryPlan, claim messaging.DeliveryClaim) (messaging.DeliveryRecord, bool, error) {
	if err := validateDeliveryPlan(key, plan); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if claim.Owner == "" || claim.TTL <= 0 {
		return messaging.DeliveryRecord{}, false, runtime.ErrCommitConflict
	}
	clientRequestID, err := messaging.StableDeliveryRequestID(key)
	if err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE delivery_ledger SET state='ambiguous',last_error_class='owner_lost',
claim_owner=NULL,claim_until=NULL,version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND state='sending' AND claim_until<=now()`, key.TenantID, key.DeliveryKey, key.SegmentNo); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	record := messaging.DeliveryRecord{DeliveryKey: key, Plan: plan, ClientRequestID: clientRequestID}
	err = s.db.QueryRowContext(ctx, `INSERT INTO delivery_ledger(
tenant_id,delivery_key,segment_no,state,renderer_version,format_version,content_digest,segment_count,attempt,client_request_id,claim_owner,claim_until)
VALUES($1,$2,$3,'sending',$4,$5,$6,$7,1,$8,$9,now()+($10 * interval '1 microsecond'))
ON CONFLICT (tenant_id,delivery_key,segment_no) DO UPDATE SET
  state='sending',attempt=delivery_ledger.attempt+1,claim_owner=EXCLUDED.claim_owner,claim_until=EXCLUDED.claim_until,
  version=delivery_ledger.version+1,updated_at=now()
WHERE delivery_ledger.state IN ('pending','retry_wait') AND delivery_ledger.not_before<=now()
  AND delivery_ledger.renderer_version=EXCLUDED.renderer_version
  AND delivery_ledger.format_version=EXCLUDED.format_version
  AND delivery_ledger.content_digest=EXCLUDED.content_digest
  AND delivery_ledger.segment_count=EXCLUDED.segment_count
  AND delivery_ledger.client_request_id=EXCLUDED.client_request_id
RETURNING COALESCE(provider_message_id,''),state,attempt,reconcile_attempt,not_before,COALESCE(last_error_class,''),client_request_id,
COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'::timestamptz),version,updated_at`,
		key.TenantID, key.DeliveryKey, key.SegmentNo, plan.RendererVersion, plan.FormatVersion, plan.ContentDigest, plan.SegmentCount,
		clientRequestID, claim.Owner, claim.TTL.Microseconds()).
		Scan(&record.ProviderMessageID, &record.State, &record.Attempt, &record.ReconcileAttempt, &record.NotBefore, &record.LastErrorClass,
			&record.ClientRequestID, &record.ClaimOwner, &record.ClaimUntil, &record.Version, &record.UpdatedAt)
	if err == nil {
		return record, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, false, err
	}
	existing, getErr := s.GetDelivery(ctx, key)
	if getErr != nil {
		return messaging.DeliveryRecord{}, false, getErr
	}
	if existing.Plan != plan {
		return messaging.DeliveryRecord{}, false, runtime.ErrIdempotencyCollision
	}
	return existing, false, nil
}

func (s *Store) RenewDeliveryClaim(ctx context.Context, in messaging.DeliveryRecord, ttl time.Duration) (messaging.DeliveryRecord, error) {
	if ttl <= 0 || in.ClaimOwner == "" || in.ClientRequestID == "" {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET claim_until=now()+($7 * interval '1 microsecond'),version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$4 AND state='sending' AND claim_owner=$5
  AND client_request_id=$6 AND claim_until>now()
RETURNING claim_until,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, in.Version,
		in.ClaimOwner, in.ClientRequestID, ttl.Microseconds()).Scan(&in.ClaimUntil, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	return in, err
}

func (s *Store) FinishDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryAmbiguous && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	if in.NotBefore.IsZero() {
		in.NotBefore = time.Now().UTC()
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET provider_message_id=$4,state=$5,not_before=$6,last_error_class=$7,
claim_owner=NULL,claim_until=NULL,version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$8 AND state='sending' AND claim_owner=$9 AND client_request_id=$10
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, nullable(in.ProviderMessageID),
		in.State, in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClaimOwner, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	return in, err
}

func (s *Store) ReconcileDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	if in.NotBefore.IsZero() {
		in.NotBefore = time.Now().UTC()
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET provider_message_id=$4,state=$5,not_before=$6,last_error_class=$7,
version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$8 AND state='ambiguous' AND client_request_id=$9
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, nullable(in.ProviderMessageID),
		in.State, in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	return in, err
}

func (s *Store) DeferDeliveryReconciliation(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliveryAmbiguous || in.ReconcileAttempt < 1 || in.NotBefore.IsZero() {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET reconcile_attempt=$4,not_before=$5,last_error_class=$6,
version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$7 AND state='ambiguous'
  AND client_request_id=$8 AND reconcile_attempt=$4-1
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, in.ReconcileAttempt,
		in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	return in, err
}
```

## 外部证据处置事务原文

```go
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// DeliveryResolution requires external evidence, not a guess based on a timeout.
// NotDelivered closes the incident; retry_not_delivered explicitly authorizes
// retry only after the operator has established the provider did not deliver.
type DeliveryResolution struct {
	TenantID          string `json:"tenant_id"`
	DeliveryKey       string `json:"delivery_key"`
	SegmentNo         int    `json:"segment_no"`
	ExpectedVersion   int64  `json:"expected_version"`
	Actor             string `json:"actor"`
	EvidenceRef       string `json:"evidence_ref"`
	Decision          string `json:"decision"`
	ProviderMessageID string `json:"provider_message_id"`
}

func (in DeliveryResolution) Validate() error {
	if strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.DeliveryKey) == "" || in.SegmentNo < 0 || in.ExpectedVersion < 1 ||
		strings.TrimSpace(in.Actor) == "" || len(in.Actor) > 256 || strings.TrimSpace(in.EvidenceRef) == "" || len(in.EvidenceRef) > 1024 {
		return runtime.ErrInvalidEnvelope
	}
	if (in.Decision == "delivered" && strings.TrimSpace(in.ProviderMessageID) != "" && len(in.ProviderMessageID) <= 1024) ||
		((in.Decision == "not_delivered" || in.Decision == "retry_not_delivered") && in.ProviderMessageID == "") {
		return nil
	}
	return runtime.ErrInvalidEnvelope
}

// ResolveUncertainDelivery atomically closes an uncertain delivery and persists
// the operator's evidence. A version check fences both concurrent operators and
// the automatic reconciler. Sent deliveries can never be reopened or resent.
func (s *Store) ResolveUncertainDelivery(ctx context.Context, in DeliveryResolution) error {
	if err := in.Validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, class := "sent", "operator_confirmed_delivered"
	if in.Decision == "not_delivered" {
		state, class = "failed", "operator_confirmed_not_delivered"
	}
	if in.Decision == "retry_not_delivered" {
		state, class = "retry_wait", "operator_confirmed_not_delivered"
	}
	var version int64
	err = tx.QueryRowContext(ctx, `UPDATE delivery_ledger
SET state=$5,provider_message_id=NULLIF($6,''),last_error_class=$7,not_before=now(),version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$4
AND (state='ambiguous' OR (state='failed' AND last_error_class='reconcile_exhausted'))
RETURNING version`, in.TenantID, in.DeliveryKey, in.SegmentNo, in.ExpectedVersion, state, in.ProviderMessageID, class).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrVersionConflict
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_resolution
(tenant_id,delivery_key,segment_no,expected_version,actor,evidence_ref,decision,provider_message_id)
VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''))`, in.TenantID, in.DeliveryKey, in.SegmentNo, in.ExpectedVersion, in.Actor, in.EvidenceRef, in.Decision, in.ProviderMessageID)
	if err != nil {
		return err
	}
	if in.Decision != "not_delivered" {
		// The Redis entry may already have been ACKed on reconcile exhaustion.
		// Republish the original frozen reply with its original delivery key;
		// sent segments remain ACK-only, including the externally confirmed one.
		var outboxID string
		err = tx.QueryRowContext(ctx, `UPDATE outbox SET state='pending',claim_owner=NULL,claim_until=NULL,
published_at=NULL,next_attempt_at=now(),version=version+1
WHERE tenant_id=$1 AND kind='reply' AND idempotency_key=$2 AND state<>'claimed'
RETURNING outbox_id`, in.TenantID, in.DeliveryKey).Scan(&outboxID)
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ErrVersionConflict
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
```

# 故障恢复后继续对话 — 从零复现参考实现

> **本文是自足的复现材料包。** 一个从未见过本仓库源码的工程师，仅凭本目录下的 Markdown（尤其本文 + `FULL-GUIDE.md` + `design.md` + `CODE-APPENDIX.md`），即可实现等价系统并通过同等验收。所有表名、字段名、函数名、环境变量名、Lua/Go 标识符均与参考实现一致，并逐字取自 `migrations/000001_service_schema.up.sql` 与 `trpcservice/coordination/redis/lease.go`。引用仓库路径统一写成「（仓库对应位置，可选核对：`path`）」。

---

## 1. 复现前置与最小系统边界

**目标能力：** 单应用节点（N1）非优雅终止（SIGKILL）后，两个企业微信 Bot（Bot A→Tenant A，Bot B→Tenant B）在原会话继续正确对话：上下文回忆不丢、租户不串扰、最终回复每输入仅一条且不重发。

**必须自带的依赖（同故障域，不承诺单点故障）：**
- PostgreSQL（权威库，两节点共用同一 `TRPC_POSTGRES_DSN`）
- Redis（队列 + 协调 lease/fence，两节点共用同一 `TRPC_REDIS_ADDRESS`）
- 恶意内容探针（malware，参与 `/readyz`）
- 两个企业微信应用（Corp+Agent 不同）的凭据

**不承诺（见 `OVERVIEW.md` §6 / `FULL-GUIDE.md` §11）：** 跨主机 / 整机 / 共享 PostgreSQL·Redis 故障；模型调用次数恒为 1；下游不可去重时 P4 的绝对 exactly-once。

---

## 2. 完整 DDL（本子系统全部表 + 约束 + 索引，逐字）

> 行号标注取自 `migrations/000001_service_schema.up.sql`，便于对照但不影响照抄实现。

### 2.1 inbox（行 240）
```sql
CREATE TABLE public.inbox (
    tenant_id text NOT NULL,
    channel text NOT NULL,
    external_account_id text NOT NULL,
    external_message_id text NOT NULL,
    request_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text,
    input_seq bigint,
    state text NOT NULL,
    payload_ref text NOT NULL,
    payload_digest text NOT NULL,
    terminal_reason text,
    result_ref text,
    key_version bigint NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    external_chat_id text DEFAULT ''::text NOT NULL,
    external_user_id text DEFAULT ''::text NOT NULL,
    CONSTRAINT inbox_input_seq_check CHECK (((input_seq IS NULL) OR (input_seq >= 1))),
    CONSTRAINT inbox_key_version_check CHECK ((key_version >= 1)),
    CONSTRAINT inbox_payload_digest_check CHECK ((payload_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_state_check CHECK ((state = ANY (ARRAY['preprocess_pending'::text, 'dispatch_pending'::text, 'dispatch_ready'::text, 'terminal'::text]))),
    CONSTRAINT inbox_version_check CHECK ((version >= 0))
);
-- 主键（行 5105）
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_pkey PRIMARY KEY (tenant_id, channel, external_account_id, external_message_id);
-- 唯一（行 5113）
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_request_id_key UNIQUE (tenant_id, request_id);
-- 外键（行 6336）
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);
```

### 2.2 session_head（行 4388）
```sql
CREATE TABLE public.session_head (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    last_fence bigint DEFAULT 0 NOT NULL,
    last_session_seq bigint DEFAULT 0 NOT NULL,
    next_input_seq bigint DEFAULT 1 NOT NULL,
    last_allocated_input_seq bigint DEFAULT 0 NOT NULL,
    state_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    summary_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_head_last_allocated_input_seq_check CHECK ((last_allocated_input_seq >= 0)),
    CONSTRAINT session_head_last_fence_check CHECK ((last_fence >= 0)),
    CONSTRAINT session_head_last_session_seq_check CHECK ((last_session_seq >= 0)),
    CONSTRAINT session_head_next_input_seq_check CHECK ((next_input_seq >= 1)),
    CONSTRAINT session_head_version_check CHECK ((version >= 0))
);
-- 主键（行 5337）
ALTER TABLE ONLY public.session_head
    ADD CONSTRAINT session_head_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id);
```

### 2.3 session_commit（行 4338）
```sql
CREATE TABLE public.session_commit (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    commit_id text NOT NULL,
    request_id text NOT NULL,
    request_digest text NOT NULL,
    input_seq bigint NOT NULL,
    stage text NOT NULL,
    outcome text NOT NULL,
    fence bigint NOT NULL,
    session_version bigint NOT NULL,
    reply_cursor text,
    result_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_commit_fence_check CHECK ((fence >= 1)),
    CONSTRAINT session_commit_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT session_commit_outcome_check CHECK ((outcome = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'waiting_confirmation'::text, 'succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]))),
    CONSTRAINT session_commit_request_digest_check CHECK ((request_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT session_commit_session_version_check CHECK ((session_version >= 1))
);
-- 主键（行 5297）
ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, commit_id);
-- 终态唯一索引（行 5621）
CREATE UNIQUE INDEX session_commit_terminal_input_idx ON public.session_commit USING btree (tenant_id, agent_app_id, session_id, input_seq) WHERE (outcome = ANY (ARRAY['succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]));
```

### 2.4 session_event（行 4365）
```sql
CREATE TABLE public.session_event (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    session_seq bigint NOT NULL,
    request_id text NOT NULL,
    input_seq bigint NOT NULL,
    event_seq bigint NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    payload_ref text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    event_payload jsonb,
    CONSTRAINT session_event_event_seq_check CHECK ((event_seq >= 1)),
    CONSTRAINT session_event_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT session_event_session_seq_check CHECK ((session_seq >= 1))
);
-- 主键（行 5313）
ALTER TABLE ONLY public.session_event
    ADD CONSTRAINT session_event_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, session_seq);
```

### 2.5 execution_record（行 3816）
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
    CONSTRAINT execution_record_park_state_check CHECK (((park_attempt >= 0) AND ((park_deadline IS NULL) OR (not_before IS NULL) OR (not_before <= park_deadline)) AND ((outcome <> 'blocked'::text) OR ((blocked_at IS NOT NULL) AND (blocked_reason = ANY (ARRAY['park_attempts_exhausted'::text, 'park_deadline_exceeded'::text])))))),
    CONSTRAINT execution_record_version_check CHECK ((version >= 0))
);
-- 主键（行 5065）
ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_pkey PRIMARY KEY (tenant_id, request_id);
-- 索引（行 5551）
CREATE INDEX execution_park_ready_idx ON public.execution_record USING btree (tenant_id, agent_app_id, session_id, input_seq, not_before) WHERE (outcome = 'pending'::text);
```

### 2.6 outbox（行 4187）
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
-- 主键（行 5217）
ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_pkey PRIMARY KEY (tenant_id, outbox_id);
-- 唯一约束（驱动幂等）
ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_tenant_id_kind_idempotency_key_key UNIQUE (tenant_id, kind, idempotency_key);
-- 索引（行 5600）
CREATE INDEX outbox_claim_idx ON public.outbox USING btree (kind, state, next_attempt_at, created_at);
```

### 2.7 preprocess_job（行 4260）
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
-- 主键（行 5257）
ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_pkey PRIMARY KEY (tenant_id, job_id);
-- 索引（行 5607 / 5614）
CREATE INDEX preprocess_job_claim_idx ON public.preprocess_job USING btree (state, not_before, lease_until, created_at);
CREATE INDEX preprocess_job_ready_idx ON public.preprocess_job USING btree (created_at) WHERE ((state = 'ready'::text) AND (dispatched_at IS NULL));
```

### 2.8 delivery_ledger（行 3761）
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
-- 主键（行 5049）
ALTER TABLE ONLY public.delivery_ledger
    ADD CONSTRAINT delivery_ledger_pkey PRIMARY KEY (tenant_id, delivery_key, segment_no);
-- 索引（行 5537 / 5544）
CREATE INDEX delivery_ledger_claim_expiry_idx ON public.delivery_ledger USING btree (claim_until) WHERE (state = 'sending'::text);
CREATE INDEX delivery_ledger_retry_idx ON public.delivery_ledger USING btree (state, not_before, updated_at) WHERE (state = ANY (ARRAY['pending'::text, 'retry_wait'::text, 'ambiguous'::text]));
```

### 2.9 channel_binding / channel_binding_locator / channel_public_route / channel_ingress_candidate
```sql
-- channel_binding（行 3574）
CREATE TABLE public.channel_binding (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    binding_id text NOT NULL,
    channel text NOT NULL,
    external_account_id text NOT NULL,
    agent_app_id text NOT NULL,
    secret_ref text NOT NULL,
    secret_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    send_secret_ref text,
    send_secret_version bigint,
    CONSTRAINT channel_binding_secret_version_check CHECK ((secret_version >= 1)),
    CONSTRAINT channel_binding_send_secret_complete CHECK ((((send_secret_ref IS NULL) AND (send_secret_version IS NULL)) OR ((length(btrim(send_secret_ref)) > 0) AND (send_secret_version >= 1))))
);
ALTER TABLE ONLY public.channel_binding ADD CONSTRAINT channel_binding_pkey PRIMARY KEY (tenant_id, config_version, binding_id);

-- channel_binding_locator（行 3595）
CREATE TABLE public.channel_binding_locator (
    opaque_binding_id text NOT NULL,
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    binding_id text NOT NULL,
    identity_secret_ref text,
    identity_secret_version bigint,
    session_secret_ref text,
    session_secret_version bigint,
    CONSTRAINT channel_binding_locator_identity_secret_complete CHECK ((((identity_secret_ref IS NULL) AND (identity_secret_version IS NULL)) OR ((length(btrim(identity_secret_ref)) > 0) AND (identity_secret_version IS NOT NULL)))),
    CONSTRAINT channel_binding_locator_identity_secret_version_check CHECK ((identity_secret_version >= 1)),
    CONSTRAINT channel_binding_locator_session_secret_complete CHECK ((((session_secret_ref IS NULL) AND (session_secret_version IS NULL)) OR ((length(btrim(session_secret_ref)) > 0) AND (session_secret_version IS NOT NULL)))),
    CONSTRAINT channel_binding_locator_session_secret_version_check CHECK ((session_secret_version >= 1))
);

-- channel_public_route（行 3643）
CREATE TABLE public.channel_public_route (
    channel text NOT NULL,
    route_key_digest text NOT NULL,
    opaque_binding_id text NOT NULL,
    binding_version bigint NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_public_route_binding_version_check CHECK ((binding_version >= 1)),
    CONSTRAINT channel_public_route_channel_check CHECK (((length(btrim(channel)) >= 1) AND (length(btrim(channel)) <= 64))),
    CONSTRAINT channel_public_route_opaque_binding_id_check CHECK (((length(btrim(opaque_binding_id)) >= 16) AND (length(btrim(opaque_binding_id)) <= 256))),
    CONSTRAINT channel_public_route_route_key_digest_check CHECK (((length(btrim(route_key_digest)) >= 16) AND (length(btrim(route_key_digest)) <= 256)))
);

-- channel_ingress_candidate（行 3615）
CREATE TABLE public.channel_ingress_candidate (
    candidate_token_digest text NOT NULL,
    opaque_binding_id text NOT NULL,
    channel text NOT NULL,
    route_key_digest text NOT NULL,
    purpose text NOT NULL,
    binding_version bigint NOT NULL,
    state text NOT NULL,
    receipt_token_digest text,
    protocol_identity_digest text,
    issued_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    verified_at timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT channel_ingress_candidate_binding_version_check CHECK ((binding_version >= 1)),
    CONSTRAINT channel_ingress_candidate_candidate_token_digest_check CHECK (((length(btrim(candidate_token_digest)) >= 16) AND (length(btrim(candidate_token_digest)) <= 256))),
    CONSTRAINT channel_ingress_candidate_check CHECK ((expires_at > issued_at)),
    CONSTRAINT channel_ingress_candidate_check1 CHECK ((((state = ANY (ARRAY['issued'::text, 'verifier_acquired'::text, 'burned'::text])) AND (receipt_token_digest IS NULL) AND (protocol_identity_digest IS NULL) AND (verified_at IS NULL)) OR ((state = ANY (ARRAY['verified'::text, 'promoted'::text])) AND (receipt_token_digest IS NOT NULL) AND (protocol_identity_digest IS NOT NULL) AND (verified_at IS NOT NULL)))),
    CONSTRAINT channel_ingress_candidate_purpose_check CHECK ((purpose = 'channel_verify'::text)),
    CONSTRAINT channel_ingress_candidate_state_check CHECK ((state = ANY (ARRAY['issued'::text, 'verifier_acquired'::text, 'verified'::text, 'promoted'::text, 'burned'::text]))),
    CONSTRAINT channel_ingress_candidate_version_check CHECK ((version >= 0))
);
```

### 2.10 其他相关表（最小字段）
```sql
-- result_payload（行 4320）
CREATE TABLE public.result_payload (
    tenant_id text NOT NULL, request_id text NOT NULL, result_ref text NOT NULL,
    result_ciphertext bytea NOT NULL, result_nonce bytea NOT NULL,
    content_digest text NOT NULL, key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT result_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT result_payload_key_version_check CHECK ((key_version >= 1)));

-- prepared_payload（行 4239）
CREATE TABLE public.prepared_payload (
    tenant_id text NOT NULL, request_id text NOT NULL, payload_ref text NOT NULL,
    source_payload_ref text NOT NULL, payload_ciphertext bytea NOT NULL, payload_nonce bytea NOT NULL,
    content_digest text NOT NULL, key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    artifact_retention_seconds bigint DEFAULT 0 NOT NULL,
    CONSTRAINT prepared_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT prepared_payload_key_version_check CHECK ((key_version > 0)));

-- inbound_payload（行 3877）
CREATE TABLE public.inbound_payload (
    tenant_id text NOT NULL, request_id text NOT NULL, payload_ref text NOT NULL,
    payload_ciphertext bytea NOT NULL, payload_nonce bytea NOT NULL,
    content_digest text NOT NULL, key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT inbound_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT inbound_payload_key_version_check CHECK ((key_version > 0)));

-- webui_message（行 4693）
CREATE TABLE public.webui_message (
    tenant_id text NOT NULL, config_version bigint NOT NULL, channel_binding_id text NOT NULL,
    external_account_id text NOT NULL, external_user_id text NOT NULL, external_chat_id text NOT NULL,
    request_id text NOT NULL, client_request_id text NOT NULL, provider_message_id text NOT NULL,
    content_ref text NOT NULL, content_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT webui_message_config_version_check CHECK ((config_version >= 1)),
    CONSTRAINT webui_message_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)));
-- 索引（行 5649）
CREATE INDEX webui_message_mailbox_idx ON public.webui_message USING btree (tenant_id, channel_binding_id, external_account_id, external_user_id, external_chat_id, created_at, provider_message_id);
```

---

## 3. 关键 SQL 函数（逐字语义 / 关键片段）

### 3.1 claim_inbox（行 314）
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
语义：平台重传 → 命中唯一键 → 读回既有行；仅当属主/摘要一致才认作同一输入；不一致视为冲突（23505）。Provider 层去重边界（I2）。

### 3.2 claim_channel_inbox（行 272）
与 `claim_inbox` 同构，额外比对 `external_chat_id` / `external_user_id`；初始态限 `preprocess_pending`/`dispatch_pending`；`p_payload_digest` 必须匹配 `^[0-9a-f]{64}$`，`external_chat_id/user_id` 长度 ≤ 512。

### 3.3 prepare_dispatch（行 1932）
关键顺序（逐字语义）：
1. `SELECT ... FROM tenant WHERE tenant_id=? FOR UPDATE`，不存在 → `P0002`。
2. `SELECT * INTO v_inbox FROM inbox WHERE tenant_id=? AND request_id=? FOR UPDATE`，不存在 → `P0002`。
3. `inbox.state='dispatch_ready'`：按 `(agent_app_id, session_id, payload_ref, agent_app_version, agent_app_revision, agent_content_digest, config_version, policy_version)` 反查 `execution_record`，找不到 → `23505`；找到则直接返回既有 `input_seq` + `accepted=true`。
4. `inbox.state='terminal'`：返回 `accepted=false` + `terminal_reason`。
5. 状态/作用域不匹配（或 `prepared_payload` 链不成立）→ `42501`。
6. tenant 非 active → inbox 置 terminal + 写 dispatch-denied 的 reply/audit outbox（`ON CONFLICT DO NOTHING`）→ `accepted=false`。
7. `tenant_version` / `active_config_version` 不匹配 → `40001`。
8. `agent_app` 必须 active 且 version/revision 匹配、revision 必须 published 且 content_digest 匹配 → 否则 `40001`；`config_snapshot` 的 `policy_version` 必须匹配 → 否则 `40001`。
9. `INSERT INTO session_head(...) ON CONFLICT DO NOTHING`；`SELECT last_allocated_input_seq+1 ... FOR UPDATE`；更新 `last_allocated_input_seq`。
10. 插入 `execution_record`（outcome 默认 'queued'）。
11. 更新 `inbox` → `dispatch_ready` + `input_seq`。
12. 写 dispatch outbox（`idempotency_key = dispatch:<request_id>`）。
语义：`input_seq` 分配与 dispatch outbox 在同一事务；重复调用收敛到同一 `input_seq`（I2）。

### 3.4 commit_turn（行 421）
关键顺序（逐字）：
1. `SELECT * FROM session_head ... FOR UPDATE`；不存在 → `P0002`。
2. `SELECT * FROM execution_record WHERE request_id=? FOR UPDATE`；scope 不一致 → `42501`。
3. 按 `commit_id` 查 `session_commit`；已存在且 digest 不同 → `23505`；相同 → 返回既有结果 `already_terminal=false`。
4. `input_seq < next_input_seq`：必须查到终态 `session_commit`，否则 `XX001`；返回该终态 `already_terminal=true`。
5. `input_seq > next_input_seq` → `55000`（input not ready）。
6. `expected_version <> head.version` → `40001`。
7. `fence < head.last_fence` → `RAISE 'stale fence' USING ERRCODE='40001'`（**迟到写被拒**）。
8. `v_new_version := version+1`；`v_new_last_seq := last_session_seq + jsonb_array_length(events)`；逐条插入 `session_event`（session_seq = last_session_seq + ordinality）。
9. `UPDATE session_head`：`version=v_new_version`、`last_fence=GREATEST(last_fence, fence)`、`last_session_seq`、`next_input_seq += (终态?1:0)`、`state_json = state_json || state_delta`。
10. 写 `session_commit`（含 fence、session_version、reply_cursor、result_ref）。
11. `UPDATE execution_record SET outcome=..., result_ref=..., version=version+1`。
12. 逐条 `INSERT outbox ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING`；`outbox_id = format('%s:%s', kind, idempotency_key)`。
终态集合（原文）：`('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')`。

### 3.5 park_execution（行 1726）
入参：`(p_tenant_id, p_request_id, p_input_seq, p_base_delay_seconds, p_max_delay_seconds, p_deadline_seconds, p_max_attempts)`。
- 校验 `base>=1`、`max>=base`、`deadline>=max`、`1<=max_attempts<=64`，否则 `22023`。
- 按 request_id 取 `execution_record`，找不到 → `P0002`；`input_seq` 不匹配 → `42501`。
- 锁 `session_head` 取 `next_input_seq`。
- 重新锁 `execution_record`；若已终态或 `p_input_seq < next`：必须存在对应终态 `session_commit`，否则 `XX001`；返回 `disposition='terminal'`。
- `p_input_seq = next`：返回 `disposition='ready'`。
- `outcome='pending'` 且 `now >= deadline` → 置 blocked/`park_deadline_exceeded` 并写 `park-blocked:<request_id>` audit outbox，返回 `'blocked'`；否则返回 `'parked'`。
- 否则 `attempt = park_attempt+1`；`attempt > max_attempts` 或 `now >= deadline` → blocked（reason `park_attempts_exhausted` / `park_deadline_exceeded`），写 audit outbox，返回 `'blocked'`。
语义：输入序号尚未轮到时把执行“停放”，直到前序终态提交；这正是 `ErrInputNotReady` 的处理路径。

### 3.6 guard_outbox_idempotency（行 1316）
```sql
CREATE FUNCTION public.guard_outbox_idempotency() RETURNS trigger
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.tenant_id || chr(31) || NEW.kind || chr(31) || NEW.idempotency_key, 0));
  PERFORM 1 FROM public.outbox WHERE tenant_id=NEW.tenant_id AND kind=NEW.kind AND idempotency_key=NEW.idempotency_key;
  IF FOUND THEN RETURN NULL; END IF;
  RETURN NEW;
```
语义：事务级咨询锁 + 存在性检查，保证 `outbox` 的 `(tenant_id, kind, idempotency_key)` 语义幂等（与唯一约束协同，避免并发重放留下的 pending）。

---

## 4. Redis 键与 Lua 契约（逐字）

**键格式（Go `keys()`）：**
```go
digest := sha256.Sum256([]byte(key.TenantID + "\x00" + key.AgentAppID + "\x00" + key.SessionID))
tag := hex.EncodeToString(digest[:])
prefix := fmt.Sprintf("trpc:%s:{%s}", m.environment, tag)
// leaseKey = prefix + ":lease"   fenceKey = prefix + ":fence"
// lease value = workerID + "|" + leaseID + "|" + fence（十进制）
```

```lua
-- acquire（KEYS[1]=lease, KEYS[2]=fence; ARGV={workerID, leaseID, ttlMillis}）
if redis.call('EXISTS', KEYS[1]) == 1 then return {0, '0'} end
redis.call('INCR', KEYS[2])
local fence = redis.call('GET', KEYS[2])
local value = ARGV[1] .. '|' .. ARGV[2] .. '|' .. fence
redis.call('PSETEX', KEYS[1], ARGV[3], value)
return {1, fence}

-- renew（KEYS={lease}; ARGV={expectedValue, ttlMillis}）
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2]); return 1

-- release（KEYS={lease}; ARGV={expectedValue}）
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])

-- ensureFence（KEYS={fence}; ARGV={minimum}）十进制比较，current<minimum 时 SET minimum
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
- `Acquire` 冲突返回 `runtime.ErrVersionConflict`；fence 解析失败/为 0 → `ErrInvariantViolation`。
- `Renew`/`Release` 必须三者（owner、leaseID、fence）整体比对（值串比较）。
- `EnsureFenceAtLeast` 拒绝 `minimum >= math.MaxInt64`（Redis INCR 是 signed 64-bit，留一个可用值）→ `ErrInvariantViolation`。
- `workerID` 不得为空、不得含 `|`；`ttl` 必须 > 0 且 ≥1ms。

---

## 5. Go 接口定义（逐字取自实现）

```go
// trpcservice/coordination/lease.go
type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type Lease struct {
	Session   SessionKey
	WorkerID  string
	LeaseID   string
	Fence     uint64
	ExpiresAt time.Time
}
type LeaseManager interface {
	Acquire(context.Context, SessionKey, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	EnsureFenceAtLeast(context.Context, SessionKey, uint64) error
}

// trpcservice/storage/session/atomic.go（节选）
type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type TerminalKey struct{ SessionKey; InputSeq uint64 }
type OpenForRunRequest struct{ SessionKey; RequestID string; InputSeq, Fence uint64 }
type SessionHead struct{ SessionKey; Version int64; LastFence, LastSessionSeq, NextInputSeq uint64; State map[string]any }
type BufferedEvent struct{ EventID, EventType, PayloadRef string; EventSeq uint64; Payload json.RawMessage }
type SessionSnapshot struct{ Head SessionHead; Events []json.RawMessage }
type StateDelta map[string]any
type SummaryCandidate struct{ SummaryID string; BaseSessionSeq uint64; LastEventID string; CutoffAt time.Time; ContentRef string }
type OutboxEvent struct{ Kind, IdempotencyKey, PayloadRef, TraceParent string; EventSeq uint64 }
type CommitTurnRequest struct{ SessionKey; RequestID, CommitID, Stage string; InputSeq, Fence uint64;
  ExpectedVersion int64; Outcome runtime.Outcome; Events []BufferedEvent; StateDelta StateDelta;
  SummaryCandidate *SummaryCandidate; ResultRef, ReplyCursor string; Outbox []OutboxEvent }
type CommitTurnResult struct{ CommitID string; Outcome runtime.Outcome; InputSeq uint64; SessionVersion int64; ResultRef, ReplyCursor string }

type AtomicSessionStore interface {
  OpenForRun(context.Context, OpenForRunRequest) (SessionHead, error)
  CommitTurn(context.Context, CommitTurnRequest) (CommitTurnResult, error)
  GetTerminalByInputSeq(context.Context, TerminalKey) (CommitTurnResult, error)
  ReadLastFence(context.Context, SessionKey) (uint64, error)
  LoadSession(context.Context, SessionKey) (SessionSnapshot, error)
}
```

会话存储 `OpenForRun` 语义（SQL）：`SELECT h.version,h.last_fence,h.last_session_seq,h.next_input_seq,h.state_json FROM session_head h JOIN execution_record e ... WHERE ...`；`input_seq < next_input_seq` → `ErrAlreadyTerminal`；`>` → `ErrInputNotReady`；`fence < last_fence` → `ErrStaleFence`。SQLSTATE→错误映射：`40001` 且含 "stale fence" → `ErrStaleFence`，否则 `ErrVersionConflict`；`23505` → `ErrCommitConflict`；`XX001` → `ErrInvariantViolation`；`55000` → `ErrInputNotReady`；`P0902` → `ErrCancelRequested`；`42501` → `ErrTenantScope`；`sql.ErrNoRows` → `ErrNotFound`。

---

## 6. 核心算法伪代码 / 参考实现

### 6.1 Worker.handle（逐字语义，仓库对应位置：`trpcservice/worker/consumer.go:181`）
```text
handle(delivery):
  key = SessionKey{delivery.Envelope.TenantID, AgentAppID, SessionID}
  persistedFence = Sessions.ReadLastFence(key)
  Leases.EnsureFenceAtLeast(key, persistedFence)        # 校准 Redis fence 到持久化值
  lease = acquire(key)                                   # 循环 Acquire，ErrVersionConflict 时 RetryWait 重试
  defer Release(lease, 1s-timeout)
  spawn renew_goroutine(lease, RenewInterval):           # ticker=LeaseTTL/3
        on Renew fail -> markLeaseLost()                 # sync.Once: 关 leaseLost + 取消执行 ctx
  beforeCommit = func(ctx):
        if leaseLost closed: return ErrLeaseLost
        if Renew(lease) fails: markLeaseLost(); return ErrLeaseLost
        return nil
  loop:
    executeErr = Executor.ExecuteWithLease(ctx, envelope, lease.Fence, beforeCommit)
    if executeErr != ErrInputNotReady: break
    parked = Parker.ParkInput(...); switch disposition:
        ParkInputReady: continue
        ParkedInput/ParkInputTerminal: executeErr=nil; break
        ParkInputBlocked: report(ErrInputBlocked); executeErr=nil; break
  if cancelRequested or executeErr==ErrCancelRequested:
        executeErr = Canceller.CancelWithLease(ctx, envelope, lease.Fence, beforeCommit)
  if leaseLost closed: return ErrLeaseLost
  if executeErr != nil: return executeErr            # 不 ACK，保留 pending 供 N2 reclaim
  return Broker.Ack(ctx, delivery)                   # 仅 terminal commit 后 ACK
```
**代码级兜底默认**（仅当调用方未设置时生效）：`LeaseTTL<=0`→5s、`RetryWait<=0`→10ms、`RenewInterval<=0`→`LeaseTTL/3`、`ReclaimInterval<=0`→1s、`ReclaimLimit<=0`→100、`DrainTimeout<=0`→30s。
**`webui-local` 的实际装配值**（部署生效值，文档与告警阈值一律引用这一列）：`LeaseTTL=30s`、`RenewInterval=10s`、`RetryWait=250ms`、`ReclaimInterval=5s`、`ReclaimLimit=100`、`DrainTimeout=30s`。
`Run()` 每 `ReclaimInterval` 执行一次 `Broker.Reclaim(consumerID, limit)`；Consume 回调执行失败**故意返回 nil** 以保持存活、让空闲 delivery 可被 reclaim。

### 6.2 Delivery.deliverSegment（逐字语义，仓库对应位置：`trpcservice/channels/delivery/service.go:109`）
```text
deliverSegment(event, adapter, plan, segmentNo, content):
  key = DeliveryKey{event.TenantID, event.DeliveryKey, segmentNo}
  claimTTL = Service.ClaimTTL or 30s
  record, acquired = Ledger.ClaimDelivery(key, plan, {Owner, TTL:claimTTL})
  if !acquired:
     sent/failed -> nil
     ambiguous   -> reconcile()
     pending/sending/retry_wait -> DeferredError{NotBefore}
  P3 barrier(Wait)                                       # claim 后、adapter 前
  result, deliverErr = deliverWithClaimRenewal(adapter, request, record, claimTTL)  # 后台按 claimTTL/3 续租
  if deliverErr is AmbiguousError: FinishDelivery(ambiguous, 'response_lost'); return
  if deliverErr is PermanentError: finishFailed(reconcile=false); return
  if deliverErr is RetryAfterError: finishRetry(retryAfter); return
  if !result.Delivered: finishRetry(ErrBackendUnavailable); return
  if result.ProviderMessageID=="": FinishDelivery(ambiguous, 'missing_provider_message_id'); return
  P4 barrier(Wait)                                       # provider receipt 后、持久化 sent 前
  record.State=sent; record.ProviderMessageID=result.ProviderMessageID
  FinishDelivery(record)
```
重试/对账默认：`MaxAttempts=8`、`MaxReconcileAttempts=8`、`DefaultRetryDelay=1s`、`MaxRetryDelay=1min`；指数退避 `1<<(attempt-1)` 上限 `MaxRetryDelay`。`reconcile()`：`NotBefore` 未到→Deferred；adapter 必须实现 `DeliveryReconciler`（否则 `deferReconciliation`）；`ReconciliationDelivered`→sent；`ReconciliationNotDelivered`→retry_wait + `reconciled_not_delivered`；`ReconciliationUnknown`→继续 defer；超过 `maxReconcileAttempts`→`finishFailed('reconcile_exhausted', reconcile=true)`。

### 6.3 Delivery Ledger ClaimDelivery 条件 CAS（仓库对应位置：`trpcservice/storage/messaging/postgres/store.go:683`）
```sql
UPDATE delivery_ledger SET state='ambiguous', last_error_class='owner_lost',
  claim_owner=NULL, claim_until=NULL, version=version+1, updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND state='sending' AND claim_until<=now();
INSERT INTO delivery_ledger(tenant_id,delivery_key,segment_no,state,renderer_version,format_version,
  content_digest,segment_count,attempt,client_request_id,claim_owner,claim_until)
VALUES($1,$2,$3,'sending',$4,$5,$6,$7,1,$8,$9, now()+($10 * interval '1 microsecond'))
ON CONFLICT (tenant_id,delivery_key,segment_no) DO UPDATE SET
  state='sending', attempt=delivery_ledger.attempt+1, claim_owner=EXCLUDED.claim_owner,
  claim_until=EXCLUDED.claim_until, version=delivery_ledger.version+1, updated_at=now()
WHERE delivery_ledger.state IN ('pending','retry_wait') AND delivery_ledger.not_before<=now()
  AND delivery_ledger.renderer_version=EXCLUDED.renderer_version
  AND delivery_ledger.format_version=EXCLUDED.format_version
  AND delivery_ledger.content_digest=EXCLUDED.content_digest
  AND delivery_ledger.segment_count=EXCLUDED.segment_count
  AND delivery_ledger.client_request_id=EXCLUDED.client_request_id
RETURNING ...;
-- 无行返回 → GetDelivery；若 existing.Plan != plan → ErrIdempotencyCollision
```
关键：`client_request_id` 由 `messaging.StableDeliveryRequestID(key)` 从 `(tenant_id, delivery_key, segment_no)` 稳定推导 —— 交给下游的去重键；owner/version/state 三重条件保证不会双发。`RenewDeliveryClaim` 要求 `version=$4 AND state='sending' AND claim_owner=$5 AND client_request_id=$6 AND claim_until>now()`，无行→`ErrVersionConflict`。`FinishDelivery` 允许目标 state ∈ {sent, retry_wait, ambiguous, failed}；`ReconcileDelivery` 要求 `state='ambiguous'`。

---

## 7. 配置全表（本子系统实际使用的环境变量）

| 变量 | 作用 | 默认/约束 |
|---|---|---|
| `TRPC_WEBUI_LOCAL_INSTANCE_ID` | 部署槽位身份 | N1/N2 必须不同 |
| （进程内）`ProcessStartID` | 每次启动随机 8 字节 hex | 不可配置，自动生成 |
| `TRPC_POSTGRES_DSN` | 业务权威库 | 两节点必须相同 |
| `TRPC_REDIS_ADDRESS` | 队列与协调 | 两节点必须相同 |
| `TRPC_LISTEN_ADDRESS` | 监听地址 | 默认 `:8080` |
| `TRPC_WECOM_LOCAL_ENABLED` | 主 Bot | `true` |
| `TRPC_WECOM_SECONDARY_LOCAL_ENABLED` | 第二 Bot | `true` 且 `(Corp ID, Agent ID)` 必须与主 Bot 不同 |
| 主 Bot：`WECOM_CORP_ID`、`WECOM_AGENT_ID`、`WECOM_APP_SECRET`、`WECOM_CALLBACK_TOKEN`、`WECOM_ENCODING_AES_KEY` | 主 Bot 凭据 | 缺失即启动失败；`WECOM_AGENT_ID` 必须可解析为正整数 |
| 次 Bot：`WECOM_SECONDARY_CORP_ID`、`WECOM_SECONDARY_AGENT_ID`、`WECOM_SECONDARY_APP_SECRET`、`WECOM_SECONDARY_CALLBACK_TOKEN`、`WECOM_SECONDARY_ENCODING_AES_KEY` | 第二 Bot 凭据 | 任一缺失 → `secondary WeCom local configuration is incomplete`；`(Corp ID, Agent ID)` 与主 Bot 相同 → `... is incompatible` |
| `TRPC_WECOM_HA_ENTRY_BACKENDS` | 入口后端 | ≥2，http，去重 |
| `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` | 探测周期 | 默认 1s，范围 [100ms, 1m] |
| `TRPC_INFLIGHT_TEST_BARRIER` | P1–P4 暂停点 | 空=关闭（生产默认） |
| `TRPC_INFLIGHT_TEST_BARRIER_INSTANCE_ID` | 命中哪个槽位 | `auto` 或具体 instance id |
| `TRPC_INFLIGHT_TEST_BARRIER_TENANT_ID` | 限定租户 | 空=任意租户 |
| `TRPC_WEBUI_LOCAL_TOKEN` / `X-TRPC-Local-Token` | 控制面鉴权 | 仅本地演练 |
| `TRPC_WECOM_REAL_ACCEPTANCE` | 真实 WeCom 验收状态 | `assumed`（默认）/ `recorded` |

---

## 8. 启动顺序（仓库对应位置：`deploy/compose/docker-compose.local.yml`，profile `wecom-ha-local`）

1. **bootstrap**：`command: ["webui-local-bootstrap"]`，一次性初始化共享 tenant/config fixture；`TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-bootstrap`；依赖 postgres/redis/qdrant healthy。
2. **node-a / node-b**：`command: ["wecom-local"]`，`restart: "no"`；共享 `TRPC_POSTGRES_DSN`、`TRPC_REDIS_ADDRESS: redis:6379`、`TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310`；`TRPC_WECOM_LOCAL_ENABLED=true`、`TRPC_WECOM_SECONDARY_LOCAL_ENABLED=true`；`TRPC_WEBUI_LOCAL_INSTANCE_ID` 分别为 `wecom-ha-node-a` / `wecom-ha-node-b`；端口 `${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}:8080` / `...NODE_B_PORT:-58089}:8080`；`depends_on: wecom-ha-bootstrap(service_completed_successfully)`、clamav、otel-collector。
3. **entry**：`command: ["wecom-ha-entry"]`，`restart: "no"`；`TRPC_WECOM_HA_ENTRY_BACKENDS: http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080`；`TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL: 1s`；端口 `${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}:8080`。**这是唯一允许暴露给 HTTPS tunnel / 企业微信控制台的端口**；两个 Bot 回调共用同一 public origin，只用 route key 区分 `local-wecom` 与 `local-wecom-secondary`。
4. PostgreSQL/Redis/Qdrant/ClamAV/otel-collector 为共享依赖（同故障域）。

---

## 9. 验证清单（复现等价系统的判定）

| # | 验证项 | 怎么做 | 通过证据 |
|---|---|---|---|
| V1 | 双节点启动 | 两节点 `/readyz`=200，`/statusz` 含 `instance_id` 与 `worker` owner | `capture_status` 断言 |
| V2 | 入口健康 | 入口 `/livez`=200、`/statusz` 两个 backend healthy | 脚本断言 |
| V3 | 非优雅故障 | `docker kill --signal=KILL` node-a | 受害容器 `State.Running==false` 且 sleep 后仍 false |
| V4 | 入口/存活节点 | 入口 `/livez`=200、node-b `/readyz`=200 | 脚本断言 |
| V5 | 原会话继续 | 两 Bot 在原会话发新消息，回忆各自代号 | 客户端可见 + `session_commit`/`delivery_ledger` 行 |
| V6 | 租户不串 | A 的回复不含 B 的代号，反之亦然 | `delivery_ledger`/`webui_message` 的 tenant 归属一致 |
| V7 | 最终回复唯一 | 每输入一条 sent，不重发 | `delivery_ledger.state='sent'` 每 segment 一行 |
| V8 | 无自动重启 | 受害节点未 restart | compose `restart:"no"` + `docker inspect` |
| V9 | 旧 fence 拒绝 | 模拟旧 N1 带旧 fence 提交 | 数据库 `stale fence` 40001 拒绝日志 |

证据字段清单：`run_id, tenant_id, binding_id, session_id, request_id, stream_entry_id, fence, delivery_key, owner, node_process_start_id`，存于独立证据目录（默认 `mktemp -d "${TMPDIR:-/tmp}/trpc-wecom-ha.XXXXXX"`，`TRPC_WECOM_HA_KEEP_ENVIRONMENT=true` 保留）。

---

## 10. 反模式（实现时务必避免）

- **只做 Redis 锁不递增 fence**：网络隔离后旧执行者迟到写会覆盖新结果。
- **先 ACK 再写库**：崩溃会丢消息；必须 terminal commit 后才 `Broker.Ack`。
- **把 Docker 重启当接管**：`restart:"no"` 且新 `ProcessStartID` 才证明“另一节点接管”。
- **绕过 P3/P4 对账**：下游不确定时必须诚实 `ambiguous`，否则双发或谎报成功。
- **健康即就绪**：`/readyz` 必须覆盖 db+redis+malware，否则“活着但不可用”的节点吞 callback。
- **请求体 tenant 字段参与路由**：tenant 只能由验签后的 binding 推出。

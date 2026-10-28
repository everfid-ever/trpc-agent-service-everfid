# 故障恢复后继续对话 — 参考实现

## 复刻前提、迁移与实现范围

本文给出本能力的实际表结构、SQL/Go 算法和配置契约。字段表优先见 design；本篇保留逐字 DDL。SQL 按依赖创建，以下是可靠性子系统摘录，不能作为一段无依赖 SQL 直接执行：tenant、agent_app、版本化配置/策略、授权等被引用的控制面表必须先存在。仓库部署使用完整迁移 000001，再依次 000002、000003、000004；不要把本文摘录再次应用到已迁移库。

完整服务还需要渠道协议验签、SDK PostgreSQL backend、租户密钥解析器、Redis broker、版本化配置加载和真实模型/provider 适配。SDK 表由迁移创建，SDK WithSkipDBInit(true)、WithEnableAsyncPersist(false)，禁止启动时另造 schema。本文是当前能力的复刻契约；不会把省略的基础设施宣称为“复制一段示例即可启动全部系统”。

## 数据结构原文

### channel_binding

```sql
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
ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_pkey PRIMARY KEY (tenant_id, config_version, binding_id);
ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_config_version_channel_external_a_key UNIQUE (tenant_id, config_version, channel, external_account_id);
ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);
ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_config_version_fkey FOREIGN KEY (tenant_id, config_version) REFERENCES public.config_snapshot(tenant_id, config_version);
```

### inbox

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
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_pkey PRIMARY KEY (tenant_id, channel, external_account_id, external_message_id);
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_request_id_key UNIQUE (tenant_id, request_id);
ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);
```

### session_head

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
ALTER TABLE ONLY public.session_head
    ADD CONSTRAINT session_head_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id);
ALTER TABLE ONLY public.session_head
    ADD CONSTRAINT session_head_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);
```

### session_commit

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
ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, commit_id);
ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_tenant_id_agent_app_id_session_id_input_seq__key UNIQUE (tenant_id, agent_app_id, session_id, input_seq, stage);
ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_tenant_id_agent_app_id_session_id_fkey FOREIGN KEY (tenant_id, agent_app_id, session_id) REFERENCES public.session_head(tenant_id, agent_app_id, session_id);
CREATE UNIQUE INDEX session_commit_terminal_input_idx ON public.session_commit USING btree (tenant_id, agent_app_id, session_id, input_seq) WHERE (outcome = ANY (ARRAY['succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]));
```

### session_events

```sql
CREATE TABLE public.session_events (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 session_id VARCHAR(255) NOT NULL,
 event JSONB NOT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
```

### inbound_payload

```sql
CREATE TABLE public.inbound_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    payload_ref text NOT NULL,
    payload_ciphertext bytea NOT NULL,
    payload_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT inbound_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT inbound_payload_key_version_check CHECK ((key_version > 0))
);
ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_pkey PRIMARY KEY (tenant_id, request_id);
ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_tenant_id_payload_ref_key UNIQUE (tenant_id, payload_ref);
ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);
```

### result_payload

```sql
CREATE TABLE public.result_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    result_ref text NOT NULL,
    result_ciphertext bytea NOT NULL,
    result_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT result_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT result_payload_key_version_check CHECK ((key_version >= 1))
);
ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_pkey PRIMARY KEY (tenant_id, request_id);
ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_result_ref_key UNIQUE (tenant_id, result_ref);
ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);
ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);
```


## 事务函数原文

以下 claim_inbox、prepare_dispatch、commit_turn 来自000001，补足去重、序号与 terminal/Outbox 同事务语义。函数引用的控制面表应先由完整迁移建立。

### claim_inbox

```sql
CREATE FUNCTION public.claim_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text) RETURNS SETOF public.inbox
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE v_row public.inbox%ROWTYPE;
BEGIN
  IF p_initial_state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR p_payload_digest !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid inbox claim' USING ERRCODE = '22023';
  END IF;
  INSERT INTO public.inbox(
    tenant_id, channel, external_account_id, external_message_id, request_id,
    agent_app_id, session_id, state, payload_ref, payload_digest, key_version
  ) VALUES (
    p_tenant_id, p_channel, p_external_account_id, p_external_message_id,
    p_request_id, p_agent_app_id, p_session_id, p_initial_state,
    p_payload_ref, p_payload_digest, p_key_version
  ) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
  SELECT * INTO v_row FROM public.inbox
    WHERE tenant_id = p_tenant_id AND channel = p_channel
      AND external_account_id = p_external_account_id
      AND external_message_id = p_external_message_id FOR UPDATE;
  IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
     OR v_row.agent_app_id <> p_agent_app_id
     OR v_row.session_id IS DISTINCT FROM p_session_id THEN
    RAISE EXCEPTION 'inbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NEXT v_row;
END;
$_$;


--
-- Name: cleanup_knowledge_backend_migration(text, text, bigint, bigint, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_rolled_back boolean; v_old_config bigint;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  SELECT EXISTS(SELECT 1 FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='rollback') INTO v_rolled_back;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'observe' OR p_at IS NULL OR p_at<v_migration.observe_until OR
     p_rollback_sync_watermark<>v_migration.verify_target_watermark OR
     (v_rolled_back AND v_tenant.active_config_version<>v_migration.source_config_version) OR
     (NOT v_rolled_back AND v_tenant.active_config_version<>v_migration.target_config_version) THEN
    RAISE EXCEPTION 'knowledge cleanup authority conflict' USING ERRCODE='40001';
  END IF;
  v_old_config := CASE WHEN v_rolled_back THEN v_migration.target_config_version ELSE v_migration.source_config_version END;
  IF EXISTS (SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') OR
     EXISTS (SELECT 1 FROM public.execution_record WHERE tenant_id=p_tenant_id AND config_version=v_old_config
       AND outcome IN ('queued','running','pending','blocked','waiting_confirmation')) THEN
    RAISE EXCEPTION 'knowledge cleanup drain is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cleanup',rollback_sync_watermark=p_rollback_sync_watermark,updated_at=p_at,version=version+1
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;
```

### prepare_dispatch

```sql
CREATE FUNCTION public.prepare_dispatch(p_tenant_id text, p_expected_tenant_version bigint, p_agent_app_id text, p_expected_app_version bigint, p_agent_app_revision bigint, p_agent_content_digest text, p_config_version bigint, p_policy_version bigint, p_request_id text, p_session_id text, p_user_id text, p_channel text, p_payload_ref text, p_traceparent text) RETURNS TABLE(input_seq bigint, accepted boolean, terminal_reason text)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_tenant_status text; v_tenant_version bigint; v_active_config bigint;
  v_app_status text; v_app_version bigint; v_current_revision bigint;
  v_revision_digest text; v_inbox public.inbox%ROWTYPE; v_input bigint;
BEGIN
  SELECT status,version,active_config_version INTO v_tenant_status,v_tenant_version,v_active_config
    FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_inbox FROM public.inbox WHERE tenant_id=p_tenant_id AND request_id=p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'inbox not found' USING ERRCODE='P0002'; END IF;
  IF v_inbox.state='dispatch_ready' THEN
    SELECT e.input_seq INTO v_input FROM public.execution_record e
      WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id
        AND e.agent_app_id=p_agent_app_id AND e.session_id=p_session_id
        AND e.payload_ref=p_payload_ref AND e.agent_app_version=p_expected_app_version
        AND e.agent_app_revision=p_agent_app_revision AND e.agent_content_digest=p_agent_content_digest
        AND e.config_version=p_config_version AND e.policy_version=p_policy_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'dispatch idempotency collision' USING ERRCODE='23505'; END IF;
    RETURN QUERY SELECT v_input,true,NULL::text; RETURN;
  END IF;
  IF v_inbox.state='terminal' THEN
    RETURN QUERY SELECT NULL::bigint,false,v_inbox.terminal_reason; RETURN;
  END IF;
  IF v_inbox.state NOT IN ('preprocess_pending','dispatch_pending')
     OR v_inbox.agent_app_id<>p_agent_app_id OR v_inbox.session_id IS DISTINCT FROM p_session_id
     OR v_inbox.channel<>p_channel
     OR (v_inbox.payload_ref<>p_payload_ref AND NOT EXISTS (
       SELECT 1 FROM public.prepared_payload p JOIN public.preprocess_job j
         ON j.tenant_id=p.tenant_id AND j.request_id=p.request_id
       WHERE p.tenant_id=p_tenant_id AND p.request_id=p_request_id
         AND p.payload_ref=p_payload_ref AND p.source_payload_ref=v_inbox.payload_ref
         AND j.state='ready' AND j.prepared_payload_ref=p.payload_ref
     )) THEN
    RAISE EXCEPTION 'inbox scope or state mismatch' USING ERRCODE='42501';
  END IF;
  IF v_tenant_status<>'active' THEN
    UPDATE public.inbox SET state='terminal',terminal_reason=v_tenant_status,version=version+1,updated_at=now()
      WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
      VALUES (p_tenant_id,format('dispatch-denied-reply:%s',p_request_id),'reply',p_request_id,1,
        format('dispatch-denied:%s:reply',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent),
      (p_tenant_id,format('dispatch-denied-audit:%s',p_request_id),'audit',p_request_id,1,
        format('dispatch-denied:%s:audit',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent)
      ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
    RETURN QUERY SELECT NULL::bigint,false,v_tenant_status; RETURN;
  END IF;
  IF v_tenant_version<>p_expected_tenant_version OR v_active_config<>p_config_version THEN
    RAISE EXCEPTION 'tenant or config version conflict' USING ERRCODE='40001';
  END IF;
  SELECT status,version,current_revision INTO v_app_status,v_app_version,v_current_revision
    FROM public.agent_app WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status<>'active' OR v_app_version<>p_expected_app_version
     OR v_current_revision<>p_agent_app_revision THEN
    RAISE EXCEPTION 'agent app binding changed' USING ERRCODE='40001';
  END IF;
  SELECT content_digest INTO v_revision_digest FROM public.agent_app_revision
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND revision=p_agent_app_revision AND state='published';
  IF NOT FOUND OR v_revision_digest<>p_agent_content_digest THEN
    RAISE EXCEPTION 'agent revision binding changed' USING ERRCODE='40001';
  END IF;
  PERFORM 1 FROM public.config_snapshot WHERE tenant_id=p_tenant_id AND config_version=p_config_version
    AND state='published' AND (payload->>'policy_version')::bigint=p_policy_version;
  IF NOT FOUND THEN RAISE EXCEPTION 'policy binding changed' USING ERRCODE='40001'; END IF;
  INSERT INTO public.session_head(tenant_id,agent_app_id,session_id)
    VALUES(p_tenant_id,p_agent_app_id,p_session_id) ON CONFLICT DO NOTHING;
  SELECT last_allocated_input_seq+1 INTO v_input FROM public.session_head
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id FOR UPDATE;
  UPDATE public.session_head SET last_allocated_input_seq=v_input,updated_at=now()
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id;
  INSERT INTO public.execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,
    agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref,traceparent)
    VALUES(p_tenant_id,p_request_id,v_tenant_version,p_agent_app_id,v_app_version,p_agent_app_revision,
      p_agent_content_digest,p_config_version,p_policy_version,p_session_id,p_user_id,p_channel,v_input,p_payload_ref,p_traceparent);
  UPDATE public.inbox SET state='dispatch_ready',input_seq=v_input,version=version+1,updated_at=now()
    WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES(p_tenant_id,format('dispatch:%s',p_request_id),'dispatch',p_request_id,1,
      format('dispatch:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent);
  RETURN QUERY SELECT v_input,true,NULL::text;
END;
$$;
```

### commit_turn

```sql
CREATE FUNCTION public.commit_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_stage text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_outcome text, p_events jsonb, p_state_delta jsonb, p_summary jsonb, p_result_ref text, p_reply_cursor text, p_outbox jsonb) RETURNS TABLE(commit_id text, outcome text, input_seq bigint, session_version bigint, result_ref text, reply_cursor text, already_terminal boolean)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_head public.session_head%ROWTYPE; v_existing public.session_commit%ROWTYPE;
  v_terminal public.session_commit%ROWTYPE; v_event jsonb; v_out jsonb;
  v_ordinal bigint; v_new_version bigint; v_new_last_seq bigint;
  v_execution public.execution_record%ROWTYPE;
BEGIN
  SELECT * INTO v_head FROM public.session_head WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'session not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_execution FROM public.execution_record WHERE tenant_id = p_tenant_id
    AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND OR v_execution.agent_app_id <> p_agent_app_id
     OR v_execution.session_id <> p_session_id OR v_execution.input_seq <> p_input_seq THEN
    RAISE EXCEPTION 'execution scope mismatch' USING ERRCODE = '42501';
  END IF;
  SELECT * INTO v_existing FROM public.session_commit WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.commit_id = p_commit_id;
  IF FOUND THEN
    IF v_existing.request_digest <> p_request_digest THEN
      RAISE EXCEPTION 'commit id collision' USING ERRCODE = '23505';
    END IF;
    RETURN QUERY SELECT v_existing.commit_id, v_existing.outcome, v_existing.input_seq,
      v_existing.session_version, v_existing.result_ref, v_existing.reply_cursor, false;
    RETURN;
  END IF;
  IF p_input_seq < v_head.next_input_seq THEN
    SELECT * INTO v_terminal FROM public.session_commit WHERE tenant_id = p_tenant_id
      AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.input_seq = p_input_seq
      AND session_commit.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');
    IF NOT FOUND THEN RAISE EXCEPTION 'terminal invariant missing' USING ERRCODE = 'XX001'; END IF;
    RETURN QUERY SELECT v_terminal.commit_id, v_terminal.outcome, v_terminal.input_seq,
      v_terminal.session_version, v_terminal.result_ref, v_terminal.reply_cursor, true;
    RETURN;
  END IF;
  IF p_input_seq > v_head.next_input_seq THEN RAISE EXCEPTION 'input not ready' USING ERRCODE = '55000'; END IF;
  IF p_expected_version <> v_head.version THEN RAISE EXCEPTION 'session version conflict' USING ERRCODE = '40001'; END IF;
  IF p_fence < v_head.last_fence THEN RAISE EXCEPTION 'stale fence' USING ERRCODE = '40001'; END IF;
  v_new_version := v_head.version + 1;
  v_new_last_seq := v_head.last_session_seq + jsonb_array_length(COALESCE(p_events, '[]'::jsonb));
  FOR v_event, v_ordinal IN SELECT value, ordinality FROM jsonb_array_elements(COALESCE(p_events, '[]'::jsonb)) WITH ORDINALITY LOOP
    INSERT INTO public.session_event(tenant_id, agent_app_id, session_id, session_seq,
      request_id, input_seq, event_seq, event_id, event_type, payload_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, v_head.last_session_seq + v_ordinal,
      p_request_id, p_input_seq, (v_event->>'event_seq')::bigint, v_event->>'event_id', v_event->>'event_type', v_event->>'payload_ref');
  END LOOP;
  UPDATE public.session_head SET version = v_new_version,
    last_fence = GREATEST(last_fence, p_fence), last_session_seq = v_new_last_seq,
    next_input_seq = next_input_seq + CASE WHEN p_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN 1 ELSE 0 END,
    state_json = state_json || COALESCE(p_state_delta, '{}'::jsonb), updated_at = now()
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id;
  IF p_summary IS NOT NULL AND p_summary <> 'null'::jsonb THEN
    INSERT INTO public.session_summary(tenant_id, agent_app_id, session_id, summary_id,
      base_session_seq, last_event_id, cutoff_at, content_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_summary->>'summary_id',
      (p_summary->>'base_session_seq')::bigint, p_summary->>'last_event_id',
      (p_summary->>'cutoff_at')::timestamptz, p_summary->>'content_ref')
    ON CONFLICT (tenant_id, agent_app_id, session_id, base_session_seq) DO NOTHING;
    UPDATE public.session_head SET summary_id = p_summary->>'summary_id'
      WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id
        AND (summary_id IS NULL OR (p_summary->>'base_session_seq')::bigint > COALESCE((SELECT base_session_seq FROM public.session_summary s WHERE s.tenant_id = p_tenant_id AND s.agent_app_id = p_agent_app_id AND s.session_id = p_session_id AND s.summary_id = session_head.summary_id), 0));
  END IF;
  INSERT INTO public.session_commit(tenant_id, agent_app_id, session_id, commit_id,
    request_id, request_digest, input_seq, stage, outcome, fence, session_version, reply_cursor, result_ref)
  VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_commit_id, p_request_id,
    p_request_digest, p_input_seq, p_stage, p_outcome, p_fence, v_new_version, p_reply_cursor, p_result_ref);
  UPDATE public.execution_record SET outcome = p_outcome, result_ref = p_result_ref,
    version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
  FOR v_out IN SELECT value FROM jsonb_array_elements(COALESCE(p_outbox, '[]'::jsonb)) LOOP
    INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq,
      idempotency_key, payload_ref, traceparent)
    VALUES (p_tenant_id, format('%s:%s', v_out->>'kind', v_out->>'idempotency_key'),
      v_out->>'kind', p_request_id, (v_out->>'event_seq')::bigint,
      v_out->>'idempotency_key', v_out->>'payload_ref', v_out->>'traceparent')
    -- Governance decisions may durably emit their audit fact before the
    -- terminal session commit references that same fact. The outbox key is
    -- the business idempotency boundary, so a replay must converge instead
    -- of leaving the execution pending on a unique-constraint error.
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  END LOOP;
  RETURN QUERY SELECT p_commit_id, p_outcome, p_input_seq, v_new_version, p_result_ref, p_reply_cursor, false;
END;
$$;
```


## Redis 完整租约实现

会话键由 tenant/app/session 的 NUL 分隔 SHA-256 构成 hash tag：`trpc:<environment>:{<digest>}:lease|fence`。lease value 是 `worker|leaseID|fence`。Acquire 只在不存在 lease 时 INCR fence + PSETEX；Renew/Release 先精确匹配完整 value。Inspect 一次 Lua 原子读取并且不公开 LeaseID。EnsureFenceAtLeast 使用十进制字符串比较避免 Lua double 精度丢失；Redis INCR 仍受有符号64位上限约束。

以下为当前 `trpcservice/coordination/redis/lease.go` 全文：

```go
// Package redis implements Redis-backed session leases and monotonic fences.
package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

var acquireScript = redisclient.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, '0'}
end
redis.call('INCR', KEYS[2])
local fence = redis.call('GET', KEYS[2])
local value = ARGV[1] .. '|' .. ARGV[2] .. '|' .. fence
redis.call('PSETEX', KEYS[1], ARGV[3], value)
return {1, fence}
`)

var renewScript = redisclient.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

var releaseScript = redisclient.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call('DEL', KEYS[1])
`)

var inspectScript = redisclient.NewScript(`
return {redis.call('GET', KEYS[1]) or '', redis.call('PTTL', KEYS[1]), redis.call('GET', KEYS[2]) or '0'}
`)

func (m *Manager) Inspect(ctx context.Context, key coordination.SessionKey) (coordination.LeaseObservation, error) {
	if err := validateKey(key); err != nil {
		return coordination.LeaseObservation{}, err
	}
	leaseKey, fenceKey := m.keys(key)
	values, err := inspectScript.Run(ctx, m.client, []string{leaseKey, fenceKey}).Slice()
	if err != nil {
		return coordination.LeaseObservation{}, err
	}
	if len(values) != 3 {
		return coordination.LeaseObservation{}, runtime.ErrInvariantViolation
	}
	observation := coordination.LeaseObservation{Session: key, ObservedAt: time.Now().UTC()}
	observation.Fence, err = strconv.ParseUint(fmt.Sprint(values[2]), 10, 64)
	if err != nil {
		return observation, runtime.ErrInvariantViolation
	}
	value := fmt.Sprint(values[0])
	if value == "" {
		return observation, nil
	}
	parts := strings.Split(value, "|")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return observation, runtime.ErrInvariantViolation
	}
	observation.Fence, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil || observation.Fence == 0 {
		return observation, runtime.ErrInvariantViolation
	}
	remaining := asInt64(values[1])
	if remaining < 0 {
		return observation, runtime.ErrInvariantViolation
	}
	observation.WorkerID, observation.Held, observation.RemainingMillis = parts[0], true, remaining
	return observation, nil
}

var ensureFenceScript = redisclient.NewScript(`
local function normalize(value)
  local normalized = string.gsub(value, '^0+', '')
  if normalized == '' then return '0' end
  return normalized
end
local function decimal_less(left, right)
  left = normalize(left)
  right = normalize(right)
  if string.len(left) ~= string.len(right) then
    return string.len(left) < string.len(right)
  end
  return left < right
end
local current = redis.call('GET', KEYS[1]) or '0'
local minimum = ARGV[1]
if decimal_less(current, minimum) then
  redis.call('SET', KEYS[1], minimum)
end
return 1
`)

type Manager struct {
	client      redisclient.UniversalClient
	environment string
}

func New(client redisclient.UniversalClient, environment string) (*Manager, error) {
	if client == nil || !validSegment(environment) {
		return nil, runtime.ErrInvalidEnvelope
	}
	return &Manager{client: client, environment: environment}, nil
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

func (m *Manager) Renew(ctx context.Context, lease coordination.Lease, ttl time.Duration) (coordination.Lease, error) {
	if err := validate(lease.Session, lease.WorkerID, ttl); err != nil || lease.LeaseID == "" || lease.Fence == 0 {
		if err != nil {
			return coordination.Lease{}, err
		}
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	leaseKey, _ := m.keys(lease.Session)
	matched, err := renewScript.Run(ctx, m.client, []string{leaseKey}, leaseValue(lease), ttl.Milliseconds()).Int64()
	if err != nil {
		return coordination.Lease{}, err
	}
	if matched != 1 {
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}

func (m *Manager) Release(ctx context.Context, lease coordination.Lease) error {
	if validateKey(lease.Session) != nil || lease.WorkerID == "" || lease.LeaseID == "" || lease.Fence == 0 {
		return runtime.ErrLeaseLost
	}
	leaseKey, _ := m.keys(lease.Session)
	matched, err := releaseScript.Run(ctx, m.client, []string{leaseKey}, leaseValue(lease)).Int64()
	if err != nil {
		return err
	}
	if matched != 1 {
		return runtime.ErrLeaseLost
	}
	return nil
}

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

func leaseValue(lease coordination.Lease) string {
	return lease.WorkerID + "|" + lease.LeaseID + "|" + strconv.FormatUint(lease.Fence, 10)
}

func validate(key coordination.SessionKey, workerID string, ttl time.Duration) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if workerID == "" || strings.Contains(workerID, "|") || ttl <= 0 || ttl.Milliseconds() < 1 {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func validateKey(key coordination.SessionKey) error {
	if key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func newLeaseID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func asInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseInt(string(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}

func validSegment(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

var _ coordination.LeaseManager = (*Manager)(nil)
```


## SDK 与身份接口

当前身份及会话强制点完整代码：


```go
// Package identity derives tenant-scoped internal user and session IDs from
// verified provider identities without persisting raw external IDs as keys.
package identity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Result struct {
	UserID, SessionID string
}

type Mapper struct{ Secrets secrets.Provider }

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

func (m Mapper) resolve(ctx context.Context, binding channel.VerifiedBinding, purpose secrets.Purpose, ref secrets.SecretRef) ([]byte, error) {
	value, err := m.Secrets.Resolve(ctx, secrets.Scope{
		TenantID: binding.TenantID, Subject: binding.TenantID, Purpose: purpose,
		ResourceID: binding.TenantID, ResourceVersion: ref.Version,
	}, ref)
	if err != nil {
		return nil, err
	}
	if value.Version != ref.Version || len(value.Bytes) == 0 {
		return nil, runtime.ErrVersionMismatch
	}
	return append([]byte(nil), value.Bytes...), nil
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

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
```


```go
package postgres

// This file defines the official trpc-agent-go session/postgres reuse
// boundary. The platform's multi-tenant session dimension is encoded into the
// SDK's app_name field (the official table key is app_name + user_id +
// session_id and carries no tenant/agent-app column), and every read is
// guarded by a fail-closed tenant hook. The official service owns the raw
// event/state persistence; the platform coordination contract (fence /
// version / outbox) stays in a service-owned coordination table (see
// docs/design/6.module-boundaries.md §8.1).

import (
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
)

// TenantAppSep separates tenantID from agentAppID inside the SDK app_name
// dimension.
const TenantAppSep = "/"

// TenantKey encodes the platform multi-tenant dimension into the official
// session.Key. userID/sessionID map 1:1 onto the SDK fields; tenantID +
// agentAppID collapse into app_name.
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
func ParseTenantApp(appName string) (tenantID, agentAppID string, ok bool) {
	i := strings.Index(appName, TenantAppSep)
	if i <= 0 || i == len(appName)-1 {
		return "", "", false
	}
	rest := appName[i+1:]
	if strings.Contains(rest, TenantAppSep) {
		return "", "", false
	}
	return appName[:i], rest, true
}

// TenantGetSessionHook returns a GetSessionHook that only admits reads whose
// app_name exactly matches the tenant/agent-app scope, failing closed on any
// other key. This enforces cross-tenant read isolation on top of the official
// backend, which has no native tenant column.
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
func NewTenantScopedOfficialSessionService(tenantID, agentAppID, dsn string) (*sessionpostgres.Service, error) {
	svc, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithGetSessionHook(TenantGetSessionHook(tenantID, agentAppID)),
	)
	if err != nil {
		return nil, fmt.Errorf("new official session postgres service: %w", err)
	}
	return svc, nil
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


## 核对清单

每个渠道外部消息唯一身份映射至一个 request；同 tenant/app/session 可恢复 SDK 历史；新 fence 不低于持久基准；frozen result 不再调用模型；旧 fence 提交失败；ReplyRoute 不串账号；P4 不确定结果不得被推断为已送达或未送达。真实客户端可见性必须另有证据。

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

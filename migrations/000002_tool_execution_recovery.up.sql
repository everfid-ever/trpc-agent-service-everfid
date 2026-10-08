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

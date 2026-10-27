BEGIN;
ALTER TABLE public.execution_record ADD COLUMN model_request_count bigint NOT NULL DEFAULT 0 CHECK(model_request_count>=0);
CREATE TABLE public.execution_attempt (
 tenant_id text NOT NULL, request_id text NOT NULL, attempt_no bigint NOT NULL CHECK(attempt_no>0),
 worker_id text NOT NULL CHECK(length(btrim(worker_id))>0), fence bigint NOT NULL CHECK(fence>0),
 state text NOT NULL CHECK(state IN ('running','completed','retryable','superseded')),
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 CHECK((state='running' AND finished_at IS NULL) OR (state<>'running' AND finished_at IS NOT NULL)),
 PRIMARY KEY(tenant_id,request_id,attempt_no),
 FOREIGN KEY(tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);
CREATE TABLE public.tool_execution_result (
 tenant_id text NOT NULL, request_id text NOT NULL, tool_call_id text NOT NULL,
 result_ref text NOT NULL, content_digest text NOT NULL CHECK(content_digest ~ '^[0-9a-f]{64}$'),
 ciphertext bytea NOT NULL, nonce bytea NOT NULL CHECK(octet_length(nonce)=12), key_version bigint NOT NULL CHECK(key_version>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,request_id,tool_call_id),
 FOREIGN KEY(tenant_id,request_id,tool_call_id) REFERENCES public.tool_execution(tenant_id,request_id,tool_call_id)
);
CREATE TABLE public.local_note (
 tenant_id text NOT NULL, business_key text NOT NULL, request_id text NOT NULL,
 note_id text NOT NULL, args_digest text NOT NULL CHECK(args_digest ~ '^[0-9a-f]{64}$'),
 ciphertext bytea NOT NULL, nonce bytea NOT NULL CHECK(octet_length(nonce)=12), key_version bigint NOT NULL CHECK(key_version>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,business_key), UNIQUE(tenant_id,note_id)
);
COMMIT;

BEGIN;
CREATE TABLE public.execution_completion (
 tenant_id text NOT NULL, request_id text NOT NULL,
 envelope_digest text NOT NULL CHECK(envelope_digest ~ '^[0-9a-f]{64}$'),
 content_digest text NOT NULL CHECK(content_digest ~ '^[0-9a-f]{64}$'),
 ciphertext bytea NOT NULL, nonce bytea NOT NULL CHECK(octet_length(nonce)=12),
 key_version bigint NOT NULL CHECK(key_version>0), fence bigint NOT NULL CHECK(fence>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,request_id),
 FOREIGN KEY(tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);
COMMIT;

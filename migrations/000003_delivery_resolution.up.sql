BEGIN;
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
COMMIT;

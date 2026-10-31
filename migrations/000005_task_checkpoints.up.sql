BEGIN;

CREATE TABLE public.task_plan (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    version bigint NOT NULL CHECK (version > 0),
    input_digest text NOT NULL CHECK (length(input_digest) = 64),
    key_version bigint NOT NULL CHECK (key_version > 0),
    phase text NOT NULL CHECK (phase IN ('open','sealed')),
    final_ref text,
    ciphertext bytea NOT NULL,
    nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,request_id),
    FOREIGN KEY (tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id),
    CHECK ((phase='open' AND final_ref IS NULL) OR (phase='sealed' AND length(final_ref)>0))
);

-- Every mutation is an immutable encrypted snapshot. Plan creation itself is
-- version 1 in task_plan; checkpoint history begins with the first mutation.
CREATE TABLE public.task_checkpoint (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    version bigint NOT NULL CHECK (version > 1),
    fence bigint NOT NULL CHECK (fence > 0),
    input_digest text NOT NULL,
    key_version bigint NOT NULL CHECK (key_version > 0),
    phase text NOT NULL CHECK (phase IN ('open','sealed')),
    final_ref text,
    ciphertext bytea NOT NULL,
    nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,request_id,version),
    FOREIGN KEY (tenant_id,request_id) REFERENCES public.task_plan(tenant_id,request_id)
);

-- A caller cannot bypass the application completion gate and submit success
-- for a participating request. Lock serializes seal/adoption with terminal.
CREATE FUNCTION public.check_task_plan_terminal() RETURNS trigger
LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $$
DECLARE p public.task_plan%ROWTYPE;
BEGIN
  IF NEW.outcome <> 'succeeded' THEN RETURN NEW; END IF;
  SELECT * INTO p FROM public.task_plan
    WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id FOR UPDATE;
  IF FOUND AND (p.phase<>'sealed' OR p.final_ref IS DISTINCT FROM NEW.result_ref OR p.fence<>NEW.fence) THEN
    RAISE EXCEPTION 'task completion evidence not sealed for this result/fence' USING ERRCODE='40001';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER task_plan_terminal_guard BEFORE INSERT ON public.session_commit
FOR EACH ROW EXECUTE FUNCTION public.check_task_plan_terminal();

COMMIT;

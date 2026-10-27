BEGIN;
DROP TABLE public.local_note;
DROP TABLE public.tool_execution_result;
DROP TABLE public.execution_attempt;
ALTER TABLE public.execution_record DROP COLUMN model_request_count;
COMMIT;

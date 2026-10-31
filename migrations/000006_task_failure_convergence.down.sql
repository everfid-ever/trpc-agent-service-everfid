BEGIN;
-- Reverting while failed plans exist is intentionally rejected; never delete
-- failure evidence or silently reopen stopped tasks as part of rollback.
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_check;
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_check CHECK
 ((phase='open' AND final_ref IS NULL) OR (phase='sealed' AND length(final_ref)>0));
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_phase_check;
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_phase_check CHECK (phase IN ('open','sealed'));
ALTER TABLE public.task_checkpoint DROP CONSTRAINT task_checkpoint_phase_check;
ALTER TABLE public.task_checkpoint ADD CONSTRAINT task_checkpoint_phase_check CHECK (phase IN ('open','sealed'));
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_writer_contract;
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_verified_seal;
ALTER TABLE public.task_plan DROP COLUMN admission_version;
ALTER TABLE public.task_plan DROP COLUMN checkpoint_version;
ALTER TABLE public.task_plan DROP COLUMN verified_steps;
COMMIT;

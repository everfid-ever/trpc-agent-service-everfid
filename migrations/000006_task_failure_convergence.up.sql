BEGIN;
-- Legacy rows remain admission_version=0. New participating writers stamp
-- every mutation; an old binary cannot drop the encrypted contract/counters
-- while incrementing version during a mixed-version rollout.
ALTER TABLE public.task_plan ADD COLUMN admission_version integer NOT NULL DEFAULT 0 CHECK (admission_version IN (0,1));
ALTER TABLE public.task_plan ADD COLUMN checkpoint_version bigint NOT NULL DEFAULT 0;
ALTER TABLE public.task_plan ADD COLUMN verified_steps integer NOT NULL DEFAULT 0 CHECK (verified_steps BETWEEN 0 AND 32);
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_writer_contract CHECK
 (admission_version=0 OR checkpoint_version=version);
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_verified_seal CHECK
 (admission_version=0 OR phase<>'sealed' OR verified_steps>0);
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_phase_check;
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_phase_check CHECK (phase IN ('open','sealed','failed'));
ALTER TABLE public.task_plan DROP CONSTRAINT task_plan_check;
ALTER TABLE public.task_plan ADD CONSTRAINT task_plan_check CHECK
 ((phase IN ('open','failed') AND final_ref IS NULL) OR (phase='sealed' AND length(final_ref)>0));
ALTER TABLE public.task_checkpoint DROP CONSTRAINT task_checkpoint_phase_check;
ALTER TABLE public.task_checkpoint ADD CONSTRAINT task_checkpoint_phase_check CHECK (phase IN ('open','sealed','failed'));
COMMIT;

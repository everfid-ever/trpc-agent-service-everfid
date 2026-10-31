BEGIN;
DROP TRIGGER IF EXISTS task_plan_terminal_guard ON public.session_commit;
DROP FUNCTION IF EXISTS public.check_task_plan_terminal();
DROP TABLE IF EXISTS public.task_checkpoint;
DROP TABLE IF EXISTS public.task_plan;
COMMIT;

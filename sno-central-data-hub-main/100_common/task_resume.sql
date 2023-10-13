--------------------------------------------------------------------
--  Purpose: resume tasks
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 23/09/2022
--------------------------------------------------------------------

--
-- 
--
select system$task_dependents_enable('TASK_INITIALIZE');

ALTER TASK TASK_INITIALIZE RESUME;

ALTER TASK V_UNMAPPED_TASK RESUME;

select system$task_dependents_enable('TASK_LOAD_TASK_ERROR_LOGS');
ALTER TASK TASK_LOAD_TASK_ERROR_LOGS RESUME;
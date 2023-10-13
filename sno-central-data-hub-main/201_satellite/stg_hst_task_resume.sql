--------------------------------------------------------------------
--  Purpose: resume tasks
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 23/09/2022
--------------------------------------------------------------------

-- NOT IN USE
-- 
--
select system$task_dependents_enable('TASK_INITIALIZE');

ALTER TASK TASK_INITIALIZE RESUME;

select system$task_dependents_enable('TASK_LOAD_TASK_ERROR_LOGS');
ALTER TASK TASK_LOAD_TASK_ERROR_LOGS RESUME;
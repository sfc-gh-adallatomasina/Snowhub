CREATE OR REPLACE TASK TASK_INITIALIZE
WAREHOUSE =&{l_target_wh}
SCHEDULE  = '&{l_cron}'
AS
EXECUTE IMMEDIATE $$
begin
    
    return 'DATA-COLLECTION-START_';
end;
$$
;

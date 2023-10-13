-------------------------------------
-- Integrations (SHOW)
-- scd type 2
-- issues fixed - dups found in stg, table name incorrect & no data in history
-------------------------------------
/*
drop table security.integration_stg;
drop table security.integration_history;

ALTER TASK TASK_INITIALIZE SUSPEND;
alter task TASK_LOAD_REST_EVENT_HISTORY_STG suspend;
ALTER TASK TASK_LOAD_REST_EVENT_HISTORY_HST suspend;
ALTER TASK TASK_LOAD_NETWORK_POLICY_DETAILS suspend;
ALTER TASK task_load_user_parameters suspend;
ALTER TASK TASK_LOAD_INTEGRATION_DETAILS suspend;
ALTER TASK task_load_managed_accounts suspend;
ALTER TASK task_load_shares suspend;
ALTER TASK task_load_integration suspend;

drop task task_load_integration;
drop procedure security.sp_integration_load();
*/

show integrations; --5
SELECT "name", "type", "category", "enabled", "comment", "created_on", current_timestamp()      FROM TABLE ( RESULT_SCAN ( last_query_id()));
     
select * from security.integrations_stg;--5 (not sure why its double, need to truncate & reload)
select * from security.integrations_history;

select effective_from, count(*) from security.integrations_history group by 1 order by 1; 
-- there is no history, hence updating a record to rerun & check

update security.integrations_history set comment ='test' where integration_name = 'AAD_PROVISIONING' and effective_to is null;

--checking data post update
select * from security.integrations_history where integration_name = 'AAD_PROVISIONING' ;

--stg to history comparison (stg contains all latest records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_STG
minus 
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_HISTORY  where effective_to is null;

--history to stg comparison (stg contains all latest records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_HISTORY where effective_to is null
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_STG;

--source to hisotry comparison
show integrations; 
SELECT "name", "type", "category", "enabled", "comment", "created_on"  FROM TABLE ( RESULT_SCAN ( last_query_id()))
MINUS
SELECT INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_HISTORY where effective_to is null
;

--hisotry to source comparison
show integrations; 
SELECT INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATIONS_HISTORY where effective_to is null
MINUS
SELECT "name", "type", "category", "enabled", "comment", "created_on"  FROM TABLE ( RESULT_SCAN ( last_query_id()));
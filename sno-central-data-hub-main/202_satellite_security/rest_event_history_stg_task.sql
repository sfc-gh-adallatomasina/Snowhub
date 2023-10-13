--  Purpose: create stage task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 20/12/22 	sayali phadtare 	task to load data 
--                               from snowflake.information_schema.rest_event_history view to rest_event_history_stg table
--22/12/2022   sayali phadtare   removed row_count
------------------------------------------------------------------------------------------


CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_rest_event_history_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
--AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
SCHEDULE  = '&{l_cron}'
As
EXECUTE IMMEDIATE $$

BEGIN 

USE ROLE ACCOUNTADMIN;

TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.rest_event_history_stg;
        
INSERT INTO &{l_target_db}.&{l_sec_schema}.rest_event_history_stg
(ORGANIZATION_NAME,	ACCOUNT_NAME,	REGION_NAME,EVENT_TIMESTAMP,EVENT_ID,EVENT_TYPE,ENDPOINT,METHOD,STATUS,ERROR_CODE,DETAILS,CLIENT_IP,ACTOR_NAME,ACTOR_DOMAIN,RESOURCE_NAME,
RESOURCE_DOMAIN,	DW_FILE_NAME,	DW_LOAD_TS) 
select
'&{l_hub_org_name}'                 as ORGANIZATION_NAME
,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.EVENT_TIMESTAMP
,s.EVENT_ID
,s.EVENT_TYPE
,s.ENDPOINT
,s.METHOD
,s.STATUS
,try_cast(s.ERROR_CODE as INTEGER)
,s.DETAILS
,s.CLIENT_IP
,s.ACTOR_NAME
,s.ACTOR_DOMAIN
,s.RESOURCE_NAME
,s.RESOURCE_DOMAIN
,'REST EVENT HISTORY'
,current_timestamp()
from table(snowflake.information_schema.rest_event_history(  rest_service_type => 'scim',
     time_range_start => dateadd('hours',-24,current_timestamp()),
     time_range_end => current_timestamp(),
     10000)) s 
where s.EVENT_TIMESTAMP >=
   to_timestamp(select ifnull( dateadd( hour, -4, max( event_timestamp ) ), '2020-01-01' ) as last_control_dt from &{l_target_db}.&{l_sec_schema}.rest_event_history_history)
;
 
return 'PASS';

END;
$$;
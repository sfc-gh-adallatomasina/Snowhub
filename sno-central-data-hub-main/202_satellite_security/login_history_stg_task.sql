--  Purpose: create stage task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	task to load data 
--                               from snowflake.account_usage.login_history view to login_history_stg table
--22/12/2022   sayali phadtare   removed row_count
-----------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_login_history_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
   
BEGIN 
   TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.login_history_stg;
        

INSERT INTO
&{l_target_db}.&{l_sec_schema}.login_history_stg
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,EVENT_ID,EVENT_TIMESTAMP,EVENT_TYPE,USER_NAME,CLIENT_IP,REPORTED_CLIENT_TYPE,REPORTED_CLIENT_VERSION,FIRST_AUTHENTICATION_FACTOR,
SECOND_AUTHENTICATION_FACTOR,IS_SUCCESS,ERROR_CODE,ERROR_MESSAGE,RELATED_EVENT_ID,CONNECTION,DW_FILE_NAME,DW_LOAD_TS) 
select
'&{l_hub_org_name}'                 as ORGANIZATION_NAME
,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.EVENT_ID,
s.EVENT_TIMESTAMP, 
s.EVENT_TYPE,
s.USER_NAME,
s.CLIENT_IP,
s.REPORTED_CLIENT_TYPE,
s.REPORTED_CLIENT_VERSION,
s.FIRST_AUTHENTICATION_FACTOR,
s.SECOND_AUTHENTICATION_FACTOR,
s.IS_SUCCESS,
s.ERROR_CODE,
s.ERROR_MESSAGE,
s.RELATED_EVENT_ID,
s.CONNECTION,
'LOGIN_HISTORY',
current_timestamp()
from snowflake.account_usage.login_history s
where s.EVENT_TIMESTAMP >= to_timestamp(select ifnull( dateadd( hour, -4, max( EVENT_TIMESTAMP ) ), '2010-01-01' ) as last_control_dt 
                                        from &{l_target_db}.&{l_sec_schema}.login_history_history)
;
END;
$$;

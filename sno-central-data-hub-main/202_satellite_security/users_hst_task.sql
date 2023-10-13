--  Purpose: create task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	task to load data 
--                               from snowflake.account_usage.users view to users_stg table
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_USERS
  --WAREHOUSE = &{l_target_wh}
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
  BEGIN
  SET ORG_NAME ='&{l_hub_org_name}' ;
  SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
  SET REGION ='&{l_satellite_region}';
  CALL &{l_target_db}.&{l_sec_schema}.sp_users_load();
END;
$$
;
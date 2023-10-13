--------------------------------------------------------------------
--  Purpose: create task task_load_task_error_logs
--
--  Revision History:
--  Date     Engineer             Description
--  -------- -------------------- ----------------------------------
--  09/08/23 Nareesh Komuravelly  Initial Version
--------------------------------------------------------------------

--
-- comment
--

CREATE OR REPLACE TASK task_load_task_error_logs
--WAREHOUSE = &{l_target_wh}
--AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
SCHEDULE  = 'USING CRON 55 6 * * * Europe/London'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
EXECUTE IMMEDIATE $$
BEGIN 
  ALTER SESSION SET TIMEZONE = 'Europe/London'; 
  INSERT INTO &{l_target_db}.&{l_target_schema}.task_error_logs 
     ( organization_name, account_name, region_name, NAME, DATABASE_NAME, SCHEMA_NAME, QUERY_ID
     , STATE, ERROR_MESSAGE, QUERY_START_TIME, COMPLETED_TIME, QUERY_TEXT, DW_LOAD_TS )
  SELECT
         '&{l_hub_org_name}'                as organization_name
        , UPPER('&{l_account_name}')        as account_name
        , current_region()                  as region_name
        , NAME
        , DATABASE_NAME
        , SCHEMA_NAME
        , QUERY_ID
        , STATE
        , ERROR_MESSAGE
        , convert_timezone('Europe/London', QUERY_START_TIME)::timestamp_ntz
        , convert_timezone('Europe/London', COMPLETED_TIME)::timestamp_ntz
        , QUERY_TEXT
        , convert_timezone('Europe/London', current_timestamp())::timestamp_ntz
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    result_limit => 10000
    , ERROR_ONLY=> TRUE))
  WHERE DATABASE_NAME = '&{l_target_db}'
  ORDER BY COMPLETED_TIME ;
  
END;
 $$;



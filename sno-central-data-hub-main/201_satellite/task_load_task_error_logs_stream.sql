use role &{l_entity_name}_sysadmin&{l_fr_suffix};

use database &{l_target_db};
use schema &{l_target_schema};
use warehouse &{l_target_wh};


CREATE OR REPLACE TASK task_load_task_error_logs_ins
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
--WAREHOUSE = &{l_target_wh}
AFTER &{l_target_db}.&{l_target_schema}.task_load_task_error_logs
WHEN SYSTEM$STREAM_HAS_DATA('task_error_logs_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
  ALTER SESSION SET TIMEZONE = 'Europe/London';
  insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.task_error_logs (
     organization_name
    , account_name               
    , region_name                
    , NAME
    , DATABASE_NAME
    , SCHEMA_NAME
    , QUERY_ID
    , STATE
    , ERROR_MESSAGE
    , QUERY_START_TIME
    , COMPLETED_TIME
    , QUERY_TEXT
    , DW_LOAD_TS
    )
  select organization_name
    , account_name               
    , region_name                
    , NAME
    , DATABASE_NAME
    , SCHEMA_NAME
    , QUERY_ID
    , STATE
    , ERROR_MESSAGE
    , QUERY_START_TIME
    , COMPLETED_TIME
    , QUERY_TEXT
    , DW_LOAD_TS
  from &{l_target_db}.&{l_target_schema}.task_error_logs_stream
;
END;
$$;

CREATE OR REPLACE TASK task_load_database_storage_usage_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_database_storage_usage_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('database_storage_usage_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.database_storage_usage_history (dw_event_shk
    ,organization_name
    ,account_name               
    ,region_name                
    ,usage_date                   
    ,database_id                  
    ,database_name                
    ,deleted                      
    ,average_database_bytes       
    ,average_failsafe_bytes       
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts) 
    select dw_event_shk
    ,organization_name
    ,account_name               
    ,region_name                
    ,usage_date                   
    ,database_id                  
    ,database_name                
    ,deleted                      
    ,average_database_bytes       
    ,average_failsafe_bytes       
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts

    from &{l_target_db}.&{l_target_schema}.database_storage_usage_history_stream;
END;
$$;

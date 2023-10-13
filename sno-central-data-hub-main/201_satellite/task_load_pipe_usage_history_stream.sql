CREATE OR REPLACE TASK task_load_pipe_usage_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_pipe_usage_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('pipe_usage_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.pipe_usage_history (dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,credits_used
    ,bytes_inserted
    ,files_inserted
    ,pipe_id
    ,pipe_name
    ,schema_name
    ,database_name
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts
) 
    select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,credits_used
    ,bytes_inserted
    ,files_inserted
    ,pipe_id
    ,pipe_name
    ,schema_name
    ,database_name
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts from &{l_target_db}.&{l_target_schema}.pipe_usage_history_stream;
END;
$$;

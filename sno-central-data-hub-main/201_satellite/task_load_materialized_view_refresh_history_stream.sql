CREATE OR REPLACE TASK task_load_materialized_view_refresh_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_materialized_view_refresh_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('materialized_view_refresh_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.materialized_view_refresh_history (dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,credits_used
    ,table_id
    ,table_name
    ,schema_id
    ,schema_name
    ,database_id
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
    ,table_id
    ,table_name
    ,schema_id
    ,schema_name
    ,database_id
    ,database_name
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts
 from &{l_target_db}.&{l_target_schema}.materialized_view_refresh_history_stream;
END;
$$;

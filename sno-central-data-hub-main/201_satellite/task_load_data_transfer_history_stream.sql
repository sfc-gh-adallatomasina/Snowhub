CREATE OR REPLACE TASK task_load_data_transfer_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_data_transfer_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('data_transfer_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.data_transfer_history (
     dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,source_cloud
    ,source_region
    ,target_cloud
    ,target_region
    ,bytes_transferred
    ,transfer_type
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts) 
select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,source_cloud
    ,source_region
    ,target_cloud
    ,target_region
    ,bytes_transferred
    ,transfer_type
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts
    from &{l_target_db}.&{l_target_schema}.data_transfer_history_stream;
END;
$$;

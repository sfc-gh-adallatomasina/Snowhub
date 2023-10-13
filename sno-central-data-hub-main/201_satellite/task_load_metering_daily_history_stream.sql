CREATE OR REPLACE TASK task_load_metering_daily_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_metering_daily_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('metering_daily_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.metering_daily_history (dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,service_type                       
    ,usage_date                         
    ,credits_used_compute               
    ,credits_used_cloud_services        
    ,credits_used                       
    ,credits_adjustment_cloud_services  
    ,credits_billed                     
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts)
    select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,service_type                       
    ,usage_date                         
    ,credits_used_compute               
    ,credits_used_cloud_services        
    ,credits_used                       
    ,credits_adjustment_cloud_services  
    ,credits_billed                     
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts from &{l_target_db}.&{l_target_schema}.metering_daily_history_stream;
END;
$$;

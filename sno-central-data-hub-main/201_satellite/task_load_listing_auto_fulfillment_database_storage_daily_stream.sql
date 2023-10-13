CREATE OR REPLACE TASK task_load_listing_auto_fulfillment_database_storage_daily_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_listing_auto_fulfillment_database_storage_daily_hst
WHEN SYSTEM$STREAM_HAS_DATA('listing_auto_fulfillment_database_storage_daily_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.listing_auto_fulfillment_database_storage_daily_history (
     dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,REGION_GROUP
    ,SNOWFLAKE_REGION
    ,USAGE_DATE
    ,DATABASE_NAME
    ,SOURCE_DATABASE_ID
    ,DELETED
    ,AVERAGE_DATABASE_BYTES
    ,AVERAGE_FAILSAFE_BYTES
    ,LISTINGS       
    ,DW_FILE_NAME
    ,DW_LOAD_TS) 
select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,REGION_GROUP
    ,SNOWFLAKE_REGION
    ,USAGE_DATE
    ,DATABASE_NAME
    ,SOURCE_DATABASE_ID
    ,DELETED
    ,AVERAGE_DATABASE_BYTES
    ,AVERAGE_FAILSAFE_BYTES
    ,LISTINGS  
    ,DW_FILE_NAME
    ,DW_LOAD_TS
from &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history_stream;
END;
$$;

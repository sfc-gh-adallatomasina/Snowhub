---------------------------------------------------------------------
--  Purpose: create task task_load_automatic_clustering_history_stg
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 09/09/2022
--------------------------------------------------------------------

--
-- comment
--

CREATE OR REPLACE TASK task_load_metering_daily_history_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
DECLARE 
l_row_count INTEGER;   
   
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';   
TRUNCATE TABLE &{l_target_db}.&{l_target_schema}.metering_daily_history_stg;

l_row_count := (SELECT COUNT(*)
                   FROM snowflake.account_usage.metering_daily_history
                   WHERE convert_timezone('Europe/London',usage_date)::timestamp_ntz >= to_timestamp(
                      select   ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) as last_control_dt
                         from    &{l_target_db}.&{l_target_schema}.metering_daily_history)); 

insert into
    &{l_target_db}.&{l_target_schema}.metering_daily_history_stg
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,SERVICE_TYPE,USAGE_DATE,CREDITS_USED_COMPUTE,
CREDITS_USED_CLOUD_SERVICES,
CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,
CREDITS_BILLED,DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS)
    select
         '&{l_hub_org_name}'                as organization_name
        , UPPER('&{l_account_name}')        as account_name
        , current_region()                  as region_name
        ,s.service_type
        ,convert_timezone('Europe/London', s.usage_date)::timestamp_ntz
        ,s.credits_used_compute
        ,s.credits_used_cloud_services
        ,s.credits_used
        ,s.credits_adjustment_cloud_services
        ,s.credits_billed
        ,'metering_daily_history'
        ,:l_row_count
        ,(convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz
    from
        snowflake.account_usage.metering_daily_history s
    where
       convert_timezone('Europe/London',s.usage_date)::timestamp_ntz >= to_timestamp(
      
          select
        ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) as last_control_dt
    from
        &{l_target_db}.&{l_target_schema}.metering_daily_history) and
         convert_timezone('Europe/London',s.usage_date)::timestamp_ntz < convert_timezone('Europe/London',current_timestamp())::date;
END;
$$;








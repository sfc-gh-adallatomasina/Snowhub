--------------------------------------------------------------------
--  Purpose: create task task_load_listing_auto_fulfillment_refresh_daily_stg after task_initialize
--
--  Revision History:
--  Date        Engineer               Description
--  -------- ------------- --------------------------------------------------------------------
--  11/07/2023 Nareesh Komuravelly     Initial version for private listings
-----------------------------------------------------------------------------------------------

CREATE  OR REPLACE TASK task_load_listing_auto_fulfillment_refresh_daily_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
DECLARE 
l_row_count INTEGER;   
   
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';   

TRUNCATE TABLE &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_stg;

l_row_count := (SELECT COUNT(*)
                FROM snowflake.data_sharing_usage.listing_auto_fulfillment_refresh_daily
                WHERE convert_timezone('Europe/London',usage_date)::timestamp_ntz >= 
                      to_timestamp(select ifnull( dateadd( day, -7, max(convert_timezone('Europe/London',usage_date)::timestamp_ntz) ), '2020-01-01' )
                                   from &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history));

INSERT into  &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_stg (ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME , REGION_GROUP
       , SNOWFLAKE_REGION, USAGE_DATE, FULFILLMENT_GROUP_NAME, BYTES_TRANSFERRED, CREDITS_USED, DATABASES, LISTINGS, DW_FILE_NAME, DW_FILE_ROW_NO, DW_LOAD_TS) 
  SELECT 
         '&{l_hub_org_name}'               as organization_name
       , UPPER('&{l_account_name}')        as account_name
       , current_region()                  as region_name      
       , s.REGION_GROUP
       , s.SNOWFLAKE_REGION
       , s.USAGE_DATE
       , s.FULFILLMENT_GROUP_NAME
       , s.BYTES_TRANSFERRED
       , s.CREDITS_USED
       , s.DATABASES
       , s.LISTINGS
       , 'listing_auto_fulfillment_refresh_daily'
       , :l_row_count
       , (convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz
  FROM  snowflake.data_sharing_usage.listing_auto_fulfillment_refresh_daily s
  WHERE convert_timezone('Europe/London',s.usage_date)::timestamp_ntz >= 
            to_timestamp(select ifnull( dateadd( day, -7, max(convert_timezone('Europe/London',usage_date)::timestamp_ntz)) , '2020-01-01' )
                         from &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history) 
    and convert_timezone('Europe/London',s.usage_date)::timestamp_ntz < convert_timezone('Europe/London',current_timestamp())::date;

END;
$$;





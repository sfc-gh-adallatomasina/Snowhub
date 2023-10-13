--------------------------------------------------------------------
--  Purpose: create task task_load_automatic_clustering_history_hst
--
--  Revision History:
--  Date        Engineer               Description
--  -------- ------------- --------------------------------------------------------------------
--  11/07/2023 Nareesh Komuravelly     Initial version for private listings
-----------------------------------------------------------------------------------------------

--
-- comment
--

CREATE OR REPLACE TASK task_load_listing_auto_fulfillment_database_storage_daily_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_listing_auto_fulfillment_database_storage_daily_stg
AS
EXECUTE IMMEDIATE $$
BEGIN 

ALTER SESSION SET TIMEZONE = 'Europe/London'; 

INSERT INTO &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
         sha1_binary( concat( s.account_name
                             ,'|', s.organization_name
                             ,'|', s.region_name
                             ,'|', to_char(convert_timezone('Europe/London',s.usage_date), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                             ,'|', to_char(NVL(s.SOURCE_DATABASE_ID, -1 ) )
                             ,'|', to_char(NVL(s.DATABASE_NAME, 'UNKNOWN') )
                             ,'|', to_char(NVL(s.LISTINGS, array_construct('UNKNOWN')) )
                            )
                    )   as dw_event_shk
        , s.*
    from  &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_stg s      
)
select s.DW_EVENT_SHK
     , s.ORGANIZATION_NAME
     , s.ACCOUNT_NAME               
     , s.REGION_NAME                
     , s.REGION_GROUP
     , s.SNOWFLAKE_REGION
     , s.USAGE_DATE
     , s.DATABASE_NAME
     , s.SOURCE_DATABASE_ID
     , s.DELETED
     , s.AVERAGE_DATABASE_BYTES
     , s.AVERAGE_FAILSAFE_BYTES
     , s.LISTINGS       
     , s.DW_FILE_NAME
     , (convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz    as DW_LOAD_TS
from  l_stg s
where s.dw_event_shk not in ( select dw_event_shk from &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history )
order by usage_date;  -- physically sort rows by a logical partitioning date
END;
 $$;



--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer                  Description
--  -------- ------------- --------------------------------------------------------------------
--  dd/mm/yy
--  11/07/2023 Nareesh Komuravelly     Initial version for private listings
------------------------------------------------------------------------------------------------


--
-- transient staging table with no retention days
--
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create transient table if not exists &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,region_group                   varchar( 250 )      not null
    ,snowflake_region               varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_name                  varchar( 250 )      not null
    ,source_database_id             number              not null
    ,deleted                        timestamp_tz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
    ,listings                       array               
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz        not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--

create  table if not exists &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history
(
     dw_event_shk                   binary( 20 )        not null
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,region_group                   varchar( 250 )      not null
    ,snowflake_region               varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_name                  varchar( 250 )      not null
    ,source_database_id             number              not null
    ,deleted                        timestamp_tz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
    ,listings                       array               
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_load_ts                     timestamp_tz        not null
)
data_retention_time_in_days = 90
;

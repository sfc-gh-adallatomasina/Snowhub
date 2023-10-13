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
create transient table if not exists &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,region_group                   varchar( 250 )      not null
    ,snowflake_region               varchar( 250 )      not null
    ,usage_date                     date                not null
    ,fulfillment_group_name         varchar( 250 )      not null
    ,bytes_transferred              float               not null
    ,credits_used                   float               not null
    ,databases                      array               not null
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

create  table if not exists &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history
(
     dw_event_shk                   binary( 20 )        not null
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,region_group                   varchar( 250 )      not null
    ,snowflake_region               varchar( 250 )      not null
    ,usage_date                     date                not null
    ,fulfillment_group_name         varchar( 250 )      not null
    ,bytes_transferred              float               not null
    ,credits_used                   float               not null
    ,databases                      array               not null
    ,listings                       array               
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_load_ts                     timestamp_tz        not null
)
data_retention_time_in_days = 90
;

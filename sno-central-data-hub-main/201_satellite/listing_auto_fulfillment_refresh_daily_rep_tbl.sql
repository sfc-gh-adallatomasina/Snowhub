--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer          Description
--  -------- ------------- ----------------------------------------
--  dd/mm/yy
--  14/07/23 Nareesh Komuravelly Initial Version
--------------------------------------------------------------------
create  table if not exists &{l_target_db}_DATA_REP.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history
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
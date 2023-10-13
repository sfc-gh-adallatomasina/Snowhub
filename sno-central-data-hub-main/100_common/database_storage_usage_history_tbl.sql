--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 29/10/2021 Will Riley
--------------------------------------------------------------------


--
-- transient staging table with no retention days
--
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create transient table if not exists &{l_target_db}.&{l_target_schema}.database_storage_usage_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_id                    number              not null
    ,database_name                  varchar( 250 )      not null
    ,deleted                        timestamp_tz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--

create  table if not exists &{l_target_db}.&{l_target_schema}.database_storage_usage_history
(
     dw_event_shk                   binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_id                    number              not null
    ,database_name                  varchar( 250 )      not null
    ,deleted                        timestamp_tz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 1
;

--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer              Description
--  -------- -------------------  ----------------------------------
--  dd/mm/yy
-- 12/06/23  Nareesh Komuravelly   Initial version - Replacement for replication_usage_history
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create transient table if not exists &{l_target_db}.&{l_target_schema}.database_replication_usage_history_stg
(
     organization_name              varchar( 250 )      null
    ,account_name                   varchar( 250 )      null
    ,region_name                    varchar( 250 )      null
    ,start_time                     timestamp_tz       null
    ,end_time                       timestamp_tz       null
    ,database_id                    number              null
    ,database_name                  varchar( 250 )      null
    ,credits_used                   NUMBER(38,9)        null
    ,bytes_transferred              float               null
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 1
;

--
-- permanent history table with retention days
--
create table if not exists &{l_target_db}.&{l_target_schema}.database_replication_usage_history
(
     dw_event_shk                   binary( 20 )        null
    ,organization_name              varchar( 250 )      null
    ,account_name                   varchar( 250 )      null
    ,region_name                    varchar( 250 )      null
    ,start_time                     timestamp_tz       null
    ,end_time                       timestamp_tz       null
    ,database_id                    number              null
    ,database_name                  varchar( 250 )      null
    ,credits_used                   NUMBER(38,9)        null
    ,bytes_transferred              float               null
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 90
;
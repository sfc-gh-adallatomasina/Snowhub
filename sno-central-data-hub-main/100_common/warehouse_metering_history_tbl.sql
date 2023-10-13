--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 29/10/2021 Will Riley  
-- 09/09/2023 Nareesh Komuravelly   Set warehouse name as NULL
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create transient table if not exists &{l_target_db}.&{l_target_schema}.warehouse_metering_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_tz       not null
    ,end_time                       timestamp_tz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      null
    ,credits_used                   NUMBER(38,9)        not null
    ,credits_used_compute           NUMBER(38,9)        not null
    ,credits_used_cloud_services    NUMBER(38,9)        not null
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 0
;

--
-- permanent history table with retention days
--
create table if not exists &{l_target_db}.&{l_target_schema}.warehouse_metering_history
(
     dw_event_shk                   binary( 20 )        not null
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_tz       not null
    ,end_time                       timestamp_tz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      null
    ,credits_used                   NUMBER(38,9)        not null
    ,credits_used_compute           NUMBER(38,9)        not null
    ,credits_used_cloud_services    NUMBER(38,9)        not null
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 1
;

ALTER TABLE &{l_target_db}.&{l_target_schema}.warehouse_metering_history_stg ALTER COLUMN WAREHOUSE_NAME DROP NOT NULL;
ALTER TABLE &{l_target_db}.&{l_target_schema}.warehouse_metering_history ALTER COLUMN WAREHOUSE_NAME DROP NOT NULL;
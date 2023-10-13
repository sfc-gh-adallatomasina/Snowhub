--  Purpose: create stage table for reader account usage for warehouse metering
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 15/03/23  sayali phadtare  stage table for raw data collection from reader account usage for warehouse metering

--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};


create transient table if not exists &{l_target_db}.&{l_target_schema}.rau_warehouse_metering_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,READER_ACCOUNT_NAME            varchar             not null
    ,start_time                     timestamp_tz        not null
    ,end_time                       timestamp_tz        not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      not null
    ,credits_used                   NUMBER(38,9)        not null
    ,credits_used_compute           NUMBER(38,9)        not null
    ,credits_used_cloud_services    NUMBER(38,9)        not null
    ,READER_ACCOUNT_DELETED_ON      timestamp_tz        
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz        not null
)
data_retention_time_in_days = 0
;

ALTER TABLE &{l_target_db}.&{l_target_schema}.rau_warehouse_metering_history_stg ALTER COLUMN WAREHOUSE_NAME DROP NOT NULL;

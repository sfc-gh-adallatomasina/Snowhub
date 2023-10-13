--  Purpose: create replication task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 15/03/23	sayali phadtare 	load data from stream to replication table 
--
-----------------------------------------------------------------------------------------------------------

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

use database &{l_target_db};
use schema &{l_target_schema};
use warehouse &{l_target_wh};
;

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_rau_warehouse_metering_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_rau_warehouse_metering_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('rau_warehouse_metering_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.rau_warehouse_metering_history (
     dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,reader_account_name
    ,start_time
    ,end_time
    ,warehouse_id
    ,warehouse_name
    ,credits_used
    ,credits_used_compute
    ,credits_used_cloud_services
    ,reader_account_deleted_on
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts
)
    select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,reader_account_name
    ,start_time
    ,end_time
    ,warehouse_id
    ,warehouse_name
    ,credits_used
    ,credits_used_compute
    ,credits_used_cloud_services
    ,reader_account_deleted_on
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts from &{l_target_db}.&{l_target_schema}.rau_warehouse_metering_history_stream
;
END;
$$;


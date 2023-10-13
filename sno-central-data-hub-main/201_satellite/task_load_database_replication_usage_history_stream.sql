--------------------------------------------------------------------
--  Purpose: create stream
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------- --------------------------------------------------------------------
--  dd/mm/yy
--  13/06/23 Nareesh Komuravelly  Initial version to replace replication_usage_history
-----------------------------------------------------------------------------------------------
use role &{l_entity_name}_sysadmin&{l_fr_suffix};

use database &{l_target_db};
use schema &{l_target_schema};
use warehouse &{l_target_wh};


CREATE OR REPLACE TASK task_load_database_replication_usage_history_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_database_replication_usage_history_hst
WHEN SYSTEM$STREAM_HAS_DATA('database_replication_usage_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert into SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.database_replication_usage_history (dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,database_id
    ,database_name
    ,credits_used
    ,bytes_transferred
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts
)
select dw_event_shk
    ,organization_name
    ,account_name
    ,region_name
    ,start_time
    ,end_time
    ,database_id
    ,database_name
    ,credits_used
    ,bytes_transferred
    ,dw_file_name
    ,dw_file_row_no
    ,dw_load_ts from &{l_target_db}.&{l_target_schema}.database_replication_usage_history_stream
;
END;
$$;

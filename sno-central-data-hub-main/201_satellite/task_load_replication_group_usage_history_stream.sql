CREATE OR REPLACE TASK TASK_LOAD_REPLICATION_GROUP_USAGE_HISTORY_INS
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_LOAD_REPLICATION_GROUP_USAGE_HISTORY_HST
WHEN SYSTEM$STREAM_HAS_DATA('replication_group_usage_history_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';

INSERT INTO SNO_CENTRAL_MONITORING_RAW_DB_DATA_REP.LANDING.REPLICATION_GROUP_USAGE_HISTORY (DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, START_TIME, END_TIME, REPLICATION_GROUP_NAME, REPLICATION_GROUP_ID, CREDITS_USED, BYTES_TRANSFERRED, DW_FILE_NAME, DW_FILE_ROW_NO, DW_LOAD_TS)
SELECT DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, START_TIME, END_TIME, REPLICATION_GROUP_NAME, REPLICATION_GROUP_ID, CREDITS_USED, BYTES_TRANSFERRED, DW_FILE_NAME, DW_FILE_ROW_NO, DW_LOAD_TS
FROM &{l_target_db}.&{l_target_schema}.REPLICATION_GROUP_USAGE_HISTORY_STREAM;

END;
$$;
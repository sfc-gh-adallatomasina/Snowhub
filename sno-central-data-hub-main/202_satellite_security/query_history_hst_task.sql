--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare  history task to load data from query_history stage to query_history table with hash key 
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_query_history_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_TIMEOUT_MS = 86400000
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_query_history_stg
AS
EXECUTE IMMEDIATE $$
BEGIN
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 86400;
INSERT INTO
    &{l_target_db}.&{l_sec_schema}.query_history_history
select
sha1_binary( concat( s.ACCOUNT_NAME
                             ,s.ORGANIZATION_NAME
                             ,s.REGION_NAME
                             ,'|', s.QUERY_ID, -1
                             ,'|', to_char(convert_timezone('Europe/London',s.END_TIME), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                             ,'|', to_char(convert_timezone('Europe/London',s.START_TIME), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    ) as DW_EVENT_SHK,
s.ORGANIZATION_NAME,
s.ACCOUNT_NAME,
s.REGION_NAME,
s.QUERY_ID,
s.QUERY_TEXT,
s.DATABASE_ID,
s.DATABASE_NAME,
s.SCHEMA_ID,
s.SCHEMA_NAME,
s.QUERY_TYPE,
s.SESSION_ID,
s.USER_NAME,
s.ROLE_NAME,
s.WAREHOUSE_ID,
s.WAREHOUSE_NAME,
s.WAREHOUSE_SIZE,
s.WAREHOUSE_TYPE,
s.CLUSTER_NUMBER,
s.QUERY_TAG,
s.EXECUTION_STATUS,
s.ERROR_CODE,
s.ERROR_MESSAGE,
s.START_TIME,
s.END_TIME,
s.TOTAL_ELAPSED_TIME,
s.BYTES_SCANNED,
s.PERCENTAGE_SCANNED_FROM_CACHE,
s.BYTES_WRITTEN,
s.BYTES_WRITTEN_TO_RESULT,
s.BYTES_READ_FROM_RESULT,
s.ROWS_PRODUCED,
s.ROWS_INSERTED,
s.ROWS_UPDATED,
s.ROWS_DELETED,
s.ROWS_UNLOADED,
s.BYTES_DELETED,
s.PARTITIONS_SCANNED,
s.PARTITIONS_TOTAL,
s.BYTES_SPILLED_TO_LOCAL_STORAGE,
s.BYTES_SPILLED_TO_REMOTE_STORAGE,
s.BYTES_SENT_OVER_THE_NETWORK,
s.COMPILATION_TIME,
s.EXECUTION_TIME,
s.QUEUED_PROVISIONING_TIME,
s.QUEUED_REPAIR_TIME,
s.QUEUED_OVERLOAD_TIME,
s.TRANSACTION_BLOCKED_TIME,
s.OUTBOUND_DATA_TRANSFER_CLOUD,
s.OUTBOUND_DATA_TRANSFER_REGION,
s.OUTBOUND_DATA_TRANSFER_BYTES,
s.INBOUND_DATA_TRANSFER_CLOUD,
s.INBOUND_DATA_TRANSFER_REGION,
s.INBOUND_DATA_TRANSFER_BYTES,
s.LIST_EXTERNAL_FILES_TIME,
s.CREDITS_USED_CLOUD_SERVICES,
s.RELEASE_VERSION,
s.EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,
s.EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,
s.EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,
s.EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,
s.EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,
s.QUERY_LOAD_PERCENT,
s.IS_CLIENT_GENERATED_STATEMENT,
s.QUERY_ACCELERATION_BYTES_SCANNED,
s.QUERY_ACCELERATION_PARTITIONS_SCANNED,
s.QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR,
s.DW_FILE_NAME,
current_timestamp() as DW_LOAD_TS
from
    &{l_target_db}.&{l_sec_schema}.query_history_stg s
where
    DW_EVENT_SHK not in
    (
        select DW_EVENT_SHK from &{l_target_db}.&{l_sec_schema}.query_history_history
    )
order by
    END_TIME  -- physically sort rows by a logical partitioning date
;
END;
$$;



--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare history task to load data from databases stage to databases history table with hash key   

--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_tables
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
--SCHEDULE  = '&{l_cron}'
AS
EXECUTE IMMEDIATE $$
DECLARE
l_row_count INTEGER;

BEGIN 
        
l_row_count := (SELECT COUNT(*) FROM snowflake.account_usage.tables); 
INSERT INTO &{l_target_db}.&{l_sec_schema}.tables_history
(ORGANIZATION_NAME,	ACCOUNT_NAME,REGION_NAME,TABLE_ID,TABLE_NAME,TABLE_SCHEMA_ID,TABLE_SCHEMA,TABLE_CATALOG_ID,TABLE_CATALOG,TABLE_OWNER,TABLE_TYPE,IS_TRANSIENT,
CLUSTERING_KEY,ROW_COUNT,BYTES,RETENTION_TIME,SELF_REFERENCING_COLUMN_NAME,REFERENCE_GENERATION,USER_DEFINED_TYPE_CATALOG,USER_DEFINED_TYPE_SCHEMA,USER_DEFINED_TYPE_NAME,
IS_INSERTABLE_INTO,IS_TYPED,COMMIT_ACTION,CREATED,LAST_ALTERED,DELETED,AUTO_CLUSTERING_ON,COMMENT,DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS) 
select
'&{l_hub_org_name}'                 as ORGANIZATION_NAME
,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME,
current_region()                   as REGION_NAME,
s.TABLE_ID,
s.TABLE_NAME,
s.TABLE_SCHEMA_ID,
s.TABLE_SCHEMA,
s.TABLE_CATALOG_ID,
s.TABLE_CATALOG,
s.TABLE_OWNER,
s.TABLE_TYPE,
s.IS_TRANSIENT,
s.CLUSTERING_KEY,
s.ROW_COUNT,
s.BYTES,
s.RETENTION_TIME,
s.SELF_REFERENCING_COLUMN_NAME,
s.REFERENCE_GENERATION,
s.USER_DEFINED_TYPE_CATALOG,
s.USER_DEFINED_TYPE_SCHEMA,
s.USER_DEFINED_TYPE_NAME,
s.IS_INSERTABLE_INTO,
s.IS_TYPED,
s.COMMIT_ACTION,
s.CREATED,
s.LAST_ALTERED,
s.DELETED,
s.AUTO_CLUSTERING_ON,
s.COMMENT,
'TABLES',
:l_row_count,
current_timestamp()
from snowflake.account_usage.tables s
;
END;
$$;

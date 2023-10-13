--  Purpose: create stage task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	task to load data 
--                               from snowflake.account_usage.databases view to databases_stg table
--22/12/2022   sayali phadtare   removed row_count
------------------------------------------------------------------------------------------


CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_databases_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
 
BEGIN 
   TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.databases_stg;
        
 
INSERT INTO &{l_target_db}.&{l_sec_schema}.databases_stg
(ORGANIZATION_NAME,	ACCOUNT_NAME,	REGION_NAME,DATABASE_ID,DATABASE_NAME,DATABASE_OWNER,IS_TRANSIENT,COMMENT,CREATED,LAST_ALTERED,DELETED,
RETENTION_TIME,	DW_FILE_NAME,	DW_LOAD_TS) 
select
'&{l_hub_org_name}'                 as ORGANIZATION_NAME
,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.DATABASE_ID
,s.DATABASE_NAME
,s.DATABASE_OWNER
,s.IS_TRANSIENT
,s.COMMENT
,s.CREATED
,LAST_ALTERED
,DELETED
,s.RETENTION_TIME
,'DATABSES'
,current_timestamp()
from snowflake.account_usage.databases s
where s.LAST_ALTERED >=
to_timestamp(select ifnull( dateadd( hour, -4, max( LAST_ALTERED ) ), '2010-01-01' ) as last_control_dt from &{l_target_db}.&{l_sec_schema}.databases_history)
;
END;
$$;




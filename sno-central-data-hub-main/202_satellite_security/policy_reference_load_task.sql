--  Purpose: create history taskt policy_reference
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/22 	sayali phadtare history task to load data  

--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_policy_reference
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
--SCHEDULE  = '&{l_cron}'
AS
EXECUTE IMMEDIATE $$
DECLARE
l_row_count INTEGER;

BEGIN 
        
l_row_count := (SELECT COUNT(*) FROM snowflake.account_usage.POLICY_REFERENCES); 

INSERT INTO &{l_target_db}.&{l_sec_schema}.policy_reference_history
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,POLICY_DB ,POLICY_SCHEMA ,POLICY_ID ,POLICY_NAME ,POLICY_KIND ,REF_DATABASE_NAME ,
REF_SCHEMA_NAME ,REF_ENTITY_NAME ,REF_ENTITY_DOMAIN ,REF_COLUMN_NAME ,REF_ARG_COLUMN_NAMES,TAG_DATABASE ,TAG_SCHEMA ,TAG_NAME ,
POLICY_STATUS,DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS  ) 
select
'&{l_hub_org_name}'                 as  ORGANIZATION_NAME,
'&{l_ACCOUNT_NAME}'                as   ACCOUNT_NAME,
current_region()                    as REGION_NAME,
s.POLICY_DB ,
s.POLICY_SCHEMA ,
s.POLICY_ID ,
s.POLICY_NAME ,
s.POLICY_KIND ,
s.REF_DATABASE_NAME ,
s.REF_SCHEMA_NAME ,
s.REF_ENTITY_NAME ,
s.REF_ENTITY_DOMAIN ,
s.REF_COLUMN_NAME ,
s.REF_ARG_COLUMN_NAMES,
s.TAG_DATABASE ,
s.TAG_SCHEMA ,
s.TAG_NAME ,
s.POLICY_STATUS,
'POLICY_REFERENCES',
:l_row_count,
current_timestamp()
from snowflake.account_usage.POLICY_REFERENCES s
;
END;
$$;

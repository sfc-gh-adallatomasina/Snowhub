--  Purpose: create task to check for password policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 08/05/2023	sayali phadtare 	task to load data 


-----------------------------------------------------------------------------------------------------------        
CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_pass_policy_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
   
BEGIN 
   TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_STG;
        

INSERT INTO &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_STG
(ORGANIZATION_NAME ,ACCOUNT_NAME ,REGION_NAME ,ID ,NAME ,SCHEMA_ID ,SCHEMA ,DATABASE_ID ,DATABASE ,OWNER ,
OWNER_ROLE_TYPE,PASSWORD_MIN_LENGTH ,PASSWORD_MAX_LENGTH ,PASSWORD_MIN_UPPER_CASE_CHARS ,PASSWORD_MIN_LOWER_CASE_CHARS ,
PASSWORD_MIN_NUMERIC_CHARS ,PASSWORD_MIN_SPECIAL_CHARS ,PASSWORD_MAX_AGE_DAYS ,PASSWORD_MAX_RETRIES ,PASSWORD_LOCKOUT_TIME_MINS ,
COMMENT ,CREATED ,LAST_ALTERED,DELETED,DW_FILE_NAME,DW_LOAD_TS) 
select
'&{l_hub_org_name}'                 as ORGANIZATION_NAME
,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.ID,
s.NAME ,
s.SCHEMA_ID ,
s.SCHEMA ,
s.DATABASE_ID ,
s.DATABASE ,
s.OWNER ,
s.OWNER_ROLE_TYPE,
s.PASSWORD_MIN_LENGTH ,
s.PASSWORD_MAX_LENGTH ,
s.PASSWORD_MIN_UPPER_CASE_CHARS ,
s.PASSWORD_MIN_LOWER_CASE_CHARS ,
s.PASSWORD_MIN_NUMERIC_CHARS ,
s.PASSWORD_MIN_SPECIAL_CHARS ,
s.PASSWORD_MAX_AGE_DAYS ,
s.PASSWORD_MAX_RETRIES ,
s.PASSWORD_LOCKOUT_TIME_MINS ,
s.COMMENT ,
s.CREATED,
s.LAST_ALTERED,
s.DELETED,
'PASSWORD_POLICY',
current_timestamp()
from snowflake.account_usage.PASSWORD_POLICIES s
where NVL( s.DELETED, s.LAST_ALTERED) >= to_timestamp(select ifnull( dateadd( hour, -4, max( NVL(DELETED,LAST_ALTERED) ) ),
'2010-01-01' ) as last_control_dt from &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_HISTORY)
;
END;
$$;



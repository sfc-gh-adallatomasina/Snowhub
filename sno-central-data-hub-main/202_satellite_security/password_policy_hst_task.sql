--  Purpose: create task to check for password policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 08/05/2023	sayali phadtare 	task to load data 

-----------------------------------------------------------------------------------------------------------        
CREATE OR REPLACE TASK  &{l_target_db}.&{l_target_schema}.task_load_pass_policy_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_pass_policy_stg
AS
EXECUTE IMMEDIATE $$
BEGIN 
 
INSERT INTO
   &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_HISTORY
with l_stg as
(
    select
        -- generate hash key to streamline processing
         sha1_binary( concat( s.ACCOUNT_NAME
                             ,s.ORGANIZATION_NAME
                             ,s.REGION_NAME
                             ,'|', to_char( ifnull( s.ID, -1 ) )
                             ,'|', to_char( s.NAME)
                             ,'|', to_char(convert_timezone('Europe/London',NVL(s.DELETED,s.LAST_ALTERED)), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    )               as DW_EVENT_SHK
        ,s.*
    from &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_STG s
    where NVL(s.DELETED,s.LAST_ALTERED) >= to_timestamp(select ifnull( dateadd( hour, -4, max( NVL(DELETED,LAST_ALTERED) ) ), '2010-01-01' )
    as last_control_dt from &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_HISTORY)
)

select
s.DW_EVENT_SHK
,s.ORGANIZATION_NAME
,s.ACCOUNT_NAME
,s.REGION_NAME
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
s.DW_FILE_NAME
,current_timestamp()  as DW_LOAD_TS
from
    l_stg s
where
    s.DW_EVENT_SHK not in
    (
        select DW_EVENT_SHK from &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_HISTORY
    )
order by
   CREATED  -- physically sort rows by a logical partitioning date
;
END;
$$;


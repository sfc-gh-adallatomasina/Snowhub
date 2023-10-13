--  Purpose: create stage task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	task to load data 
--                               from snowflake.account_usage.grants_to_roles view to grants_to_roles_stg table
--22/12/2022   sayali phadtare   removed row_count
----------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_grants_to_roles_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
   
BEGIN 
  TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.grants_to_roles_stg;
        
  INSERT INTO &{l_target_db}.&{l_sec_schema}.grants_to_roles_stg(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED_ON,MODIFIED_ON,PRIVILEGE
             ,GRANTED_ON,NAME,TABLE_CATALOG,TABLE_SCHEMA,GRANTED_TO,GRANTEE_NAME,GRANT_OPTION,GRANTED_BY,DELETED_ON,GRANTED_BY_ROLE_TYPE,DW_FILE_NAME,DW_LOAD_TS) 
   select
         '&{l_hub_org_name}'                as ORGANIZATION_NAME
        ,'&{l_ACCOUNT_NAME}'                as ACCOUNT_NAME
        ,current_region()                   as REGION_NAME
        ,s.CREATED_ON
        ,MODIFIED_ON
        ,s.PRIVILEGE
        ,s.GRANTED_ON
        ,s.NAME
        ,s.TABLE_CATALOG
        ,s.TABLE_SCHEMA
        ,s.GRANTED_TO
        ,s.GRANTEE_NAME
        ,s.GRANT_OPTION
        ,s.GRANTED_BY
        ,s.DELETED_ON
        ,s.GRANTED_BY_ROLE_TYPE
        ,'GRANTS_TO_ROLES'
        ,current_timestamp()
   from  snowflake.account_usage.grants_to_roles s
   where NVL(MODIFIED_ON, CREATED_ON)  >= to_timestamp(select ifnull( dateadd( hour, -4, max( MODIFIED_ON ) ), '2010-01-01' ) as last_control_dt
                                                            from &{l_target_db}.&{l_sec_schema}.grants_to_roles_history)
   ;
END;
$$;

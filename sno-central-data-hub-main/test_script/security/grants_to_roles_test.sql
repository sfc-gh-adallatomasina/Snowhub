-------------------------------------
--grants_to_roles --delta with changes inserted as new records
--*new 2 columns added in soruce view i.e RESOURCE_GROUP & TYPE*
-------------------------------------
select * from snowflake.account_usage.grants_to_roles; --1.8K records
select * from security.grants_to_roles_stg; --data populated but nothing for current day, 132 records
select * from security.grants_to_roles_history; --data populated & imp columns populated, 1.8K records
-- get record counts from soruce for current day changes i.e. 132, which matches stage table
select * from snowflake.account_usage.grants_to_roles   
where NVL(MODIFIED_ON, CREATED_ON)  > to_timestamp(select ifnull( dateadd( hour, -4, max( NVL(MODIFIED_ON,CREATED_ON)  ) ), '2020-01-01' ) as last_control_dt
                                                            from security.grants_to_roles_history where dw_load_ts < current_date())
;

select dw_load_ts , count(*) from security.grants_to_roles_history group by 1 order by 1;--check daily load count. ensure not every record is changing daily & is only capturing changes
select * from security.grants_to_roles_history where dw_load_ts='2023-06-23 04:39:04.199 +0100'::timestamp_tz;--checking changes for a specific date
select * from security.grants_to_roles_history 
  where PRIVILEGE ='OWNERSHIP' and granted_on='TABLE' and name='GRANTS_TO_ROLES_STG' 
    and table_catalog='SNO_CENTRAL_MONITORING_RAW_DB' and table_schema='SECURITY' and GRANTEE_NAME='SNO_CENTRAL_MONITORING_SYSADMIN_FR' 
    and GRANTED_TO='ROLE'; -- seeing changes for a specific record to see if there is a change indeed

--stage to history data comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.GRANTS_TO_ROLES_STG
MINUS
SELECT ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.GRANTS_TO_ROLES_HISTORY
;

--history to stage data comparison (stage will have extra records which we can ignore)
SELECT ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, GRANTED_BY_ROLE_TYPE from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.GRANTS_TO_ROLES_HISTORY 
where dw_load_ts > current_date()
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, GRANTED_BY_ROLE_TYPE from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.GRANTS_TO_ROLES_STG
;

--source to history comparison
select CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE  
from snowflake.account_usage.grants_to_roles 
minus
select CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE  
from security.grants_to_roles_history 
where (PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), CREATED_ON, MODIFIED_ON) IN
  (SELECT PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), CREATED_ON, MAX(MODIFIED_ON)
   FROM security.grants_to_roles_history group by 1,2,3,4,5,6,7,8,9,10)
 /* and PRIVILEGE ='APPLY ROW ACCESS POLICY' and granted_on='ACCOUNT' and name='PQ59679' 
    and table_catalog IS NULL and table_schema IS NULL and GRANTEE_NAME='ACCOUNTADMIN' 
    and GRANTED_TO='ROLE' AND GRANT_OPTION=TRUE AND GRANTED_BY IS NULL */
;

--target to soruce comparison
select CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE  
from security.grants_to_roles_history 
where (PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), CREATED_ON, MODIFIED_ON) IN
  (SELECT PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), CREATED_ON, MAX(MODIFIED_ON)
   FROM security.grants_to_roles_history group by 1,2,3,4,5,6,7,8,9,10)
minus
select CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,' '), NVL(TABLE_SCHEMA,' '), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE  
from snowflake.account_usage.grants_to_roles 
;

--example data comparison for sample record (this can be used if any diff is found)
select 'source', CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,''), NVL(TABLE_SCHEMA,''), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE 
from snowflake.account_usage.grants_to_roles 
where PRIVILEGE ='APPLY ROW ACCESS POLICY' and granted_on='ACCOUNT' and name='PQ59679' 
    and table_catalog IS NULL and table_schema IS NULL and GRANTEE_NAME='ACCOUNTADMIN' 
    and GRANTED_TO='ROLE' AND GRANT_OPTION=TRUE AND GRANTED_BY IS NULL
union
select 'target', CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, NVL(TABLE_CATALOG,''), NVL(TABLE_SCHEMA,''), GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, NVL(GRANTED_BY,' '), NVL(DELETED_ON,CURRENT_DATE()), GRANTED_BY_ROLE_TYPE  
from security.grants_to_roles_history 
where PRIVILEGE ='APPLY ROW ACCESS POLICY' and granted_on='ACCOUNT' and name='PQ59679' 
    and table_catalog IS NULL and table_schema IS NULL and GRANTEE_NAME='ACCOUNTADMIN' 
    and GRANTED_TO='ROLE' AND GRANT_OPTION=TRUE AND GRANTED_BY IS NULL
;

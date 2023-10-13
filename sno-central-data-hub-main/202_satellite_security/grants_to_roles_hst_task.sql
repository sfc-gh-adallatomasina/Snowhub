--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare  history task to load data from grants_to_roles stage to grants_to_roles history table with hash key 
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_grants_to_roles_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_grants_to_roles_stg
AS
EXECUTE IMMEDIATE $$
BEGIN

INSERT INTO &{l_target_db}.&{l_sec_schema}.grants_to_roles_history(DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON,
MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME,GRANT_OPTION, 
GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, DW_FILE_NAME, DW_LOAD_TS)
with l_stg as
(
    select -- generate hash key to streamline processing
         sha1_binary( concat( s.ACCOUNT_NAME
                             ,s.ORGANIZATION_NAME
                             ,s.REGION_NAME
                             ,'|', s.PRIVILEGE
                             ,'|', s.GRANTED_ON
                             ,'|', s.NAME
                             ,'|', NVL(s.TABLE_CATALOG,' ')
                             ,'|', NVL(s.table_schema,' ')
                             ,'|', s.GRANTEE_NAME
                             ,'|', s.GRANTED_TO
                             ,'|', to_char(convert_timezone('Europe/London',s.CREATED_ON), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM' )
                             ,'|', to_char(convert_timezone('Europe/London',s.MODIFIED_ON), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM' )
                            )
                    ) as DW_EVENT_SHK
        ,s.*
    from  &{l_target_db}.&{l_sec_schema}.grants_to_roles_stg s
    where NVL(MODIFIED_ON,CREATED_ON) >= to_timestamp( select ifnull( dateadd( hour, -4, max( NVL(MODIFIED_ON,CREATED_ON) ) ), '2010-01-01' ) as last_control_dt from  &{l_target_db}.&{l_sec_schema}.grants_to_roles_history)
)
select
     s.DW_EVENT_SHK
    ,s.ORGANIZATION_NAME
    ,s.ACCOUNT_NAME
    ,s.REGION_NAME
    ,s.CREATED_ON
    ,s.MODIFIED_ON
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
    ,s.DW_FILE_NAME
    ,current_timestamp()    as DW_LOAD_TS
from l_stg s
where s.DW_EVENT_SHK not in ( select DW_EVENT_SHK from &{l_target_db}.&{l_sec_schema}.grants_to_roles_history )
order by MODIFIED_ON  -- physically sort rows by a logical partitioning date
;
END;
$$;



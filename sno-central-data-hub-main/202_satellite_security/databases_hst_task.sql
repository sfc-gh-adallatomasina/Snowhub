--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare history task to load data from databases stage to databases history table with hash key   
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_databases_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_databases_stg
AS
EXECUTE IMMEDIATE $$
BEGIN 
 
INSERT INTO
    &{l_target_db}.&{l_sec_schema}.databases_history
with l_stg as
(
    select
        -- generate hash key to streamline processing
         sha1_binary( concat( s.ACCOUNT_NAME
                             ,s.ORGANIZATION_NAME
                             ,s.REGION_NAME
                             ,'|', to_char( ifnull( s.database_id, -1 ) )
                             ,'|', to_char(convert_timezone('Europe/London',s.LAST_ALTERED), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    )               as DW_EVENT_SHK
        ,s.*
    from &{l_target_db}.&{l_sec_schema}.databases_stg s
    where s.LAST_ALTERED >= to_timestamp(select ifnull( dateadd( hour, -4, max( LAST_ALTERED ) ), '2010-01-01' ) as last_control_dt from &{l_target_db}.&{l_sec_schema}.databases_history)
)
select
     s.DW_EVENT_SHK
    ,s.ORGANIZATION_NAME
    ,s.ACCOUNT_NAME
    ,s.REGION_NAME
    ,s.DATABASE_ID
    ,s.DATABASE_NAME
    ,s.DATABASE_OWNER
    ,s.IS_TRANSIENT
    ,s.COMMENT
    ,s.CREATED
    ,LAST_ALTERED
    ,DELETED
    ,s.RETENTION_TIME
    ,s.DW_FILE_NAME
    ,current_timestamp()   as DW_LOAD_TS
from
    l_stg s
where
    s.DW_EVENT_SHK not in
    (
        select DW_EVENT_SHK from &{l_target_db}.&{l_sec_schema}.databases_history
    )
order by
    LAST_ALTERED  -- physically sort rows by a logical partitioning date
;
END;
$$;



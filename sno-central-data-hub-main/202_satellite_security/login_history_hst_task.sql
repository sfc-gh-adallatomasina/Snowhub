--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare  history task to load data from login_history_ stage to login history table with hash key 
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_login_history_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_login_history_stg
AS
EXECUTE IMMEDIATE $$
BEGIN 
 
INSERT INTO
    &{l_target_db}.&{l_sec_schema}.login_history_history
with l_stg as
(
    select
        -- generate hash key to streamline processing
         sha1_binary( concat( s.ACCOUNT_NAME
                             ,s.ORGANIZATION_NAME
                             ,s.REGION_NAME
                             ,'|', to_char( ifnull( s.EVENT_ID, -1 ) )
                             ,'|', to_char(convert_timezone('Europe/London', s.EVENT_TIMESTAMP), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    )               as DW_EVENT_SHK
        ,s.*
    from &{l_target_db}.&{l_sec_schema}.login_history_stg s
    where s.EVENT_TIMESTAMP >= to_timestamp(select ifnull( dateadd( hour, -4, max( EVENT_TIMESTAMP ) ), '2010-01-01' ) as last_control_dt from &{l_target_db}.&{l_sec_schema}.login_history_history)
)

select
 s.DW_EVENT_SHK
,s.ORGANIZATION_NAME
,s.ACCOUNT_NAME
,s.REGION_NAME
,s.EVENT_ID
,s.EVENT_TIMESTAMP
,s.EVENT_TYPE
,s.USER_NAME
,s.CLIENT_IP
,s.REPORTED_CLIENT_TYPE
,s.REPORTED_CLIENT_VERSION
,s.FIRST_AUTHENTICATION_FACTOR
,s.SECOND_AUTHENTICATION_FACTOR
,s.IS_SUCCESS
,s.ERROR_CODE
,s.ERROR_MESSAGE
,s.RELATED_EVENT_ID
,s.CONNECTION
,s.DW_FILE_NAME
,current_timestamp() as DW_LOAD_TS
from
    l_stg s
where
    s.DW_EVENT_SHK not in
    (
        select DW_EVENT_SHK from &{l_target_db}.&{l_sec_schema}.login_history_history
    )
order by
    EVENT_TIMESTAMP  -- physically sort rows by a logical partitioning date
;
END;
$$;



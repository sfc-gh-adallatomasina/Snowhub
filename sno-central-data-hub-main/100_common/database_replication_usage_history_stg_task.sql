--------------------------------------------------------------------
--  Purpose: create task to load stage table
--
--  Revision History:
--  Date     Engineer              Description
--  -------- -------------------  ----------------------------------
--  dd/mm/yy
-- 12/06/23  Nareesh Komuravelly   Initial version - Replacement for replication_usage_history
--------------------------------------------------------------------

--
-- comment
--

CREATE OR REPLACE TASK task_load_database_replication_usage_history_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
DECLARE 
l_row_count INTEGER;   

   
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';   
TRUNCATE TABLE &{l_target_db}.&{l_target_schema}.database_replication_usage_history_stg;
          
l_row_count := (SELECT COUNT(*)
                   FROM snowflake.account_usage.database_replication_usage_history
                   where convert_timezone('Europe/London',start_time)::timestamp_ntz >= to_timestamp(
                       select   ifnull( dateadd( hour, -4, max( start_time ) ), '2020-01-01' ) as last_control_dt
                         from   &{l_target_db}.&{l_target_schema}.database_replication_usage_history)); 
INSERT into
    &{l_target_db}.&{l_target_schema}.database_replication_usage_history_stg
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,START_TIME,END_TIME,
DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED,DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS)
    select
         '&{l_hub_org_name}'                as organization_name
        , UPPER('&{l_account_name}')        as account_name
        , current_region()                  as region_name
        , (convert_timezone('Europe/London', start_time) )::timestamp_tz
    ,(convert_timezone('Europe/London', end_time) )::timestamp_tz
        ,s.database_id
        ,s.database_name
        ,s.credits_used
        ,s.bytes_transferred
        ,'database_replication_usage_history'
        ,:l_row_count
        ,(convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz
    from  snowflake.account_usage.database_replication_usage_history s
    where convert_timezone('Europe/London',s.start_time)::timestamp_ntz > NVL((SELECT convert_timezone('Europe/London',max(start_time))::timestamp_ntz FROM &{l_target_db}.&{l_target_schema}.replication_usage_history), '2020-01-01' )
      and convert_timezone('Europe/London',s.start_time)::timestamp_ntz >= to_timestamp(select  ifnull( dateadd( hour, -4, max( start_time ) ), '2020-01-01' ) as last_control_dt
                                                                                        from  &{l_target_db}.&{l_target_schema}.database_replication_usage_history) 
      and convert_timezone('Europe/London',s.start_time)::timestamp_ntz < convert_timezone('Europe/London',current_timestamp())::date;
END;
$$;        








--------------------------------------------------------------------
--  Purpose: create task task_load_automatic_clustering_history_stg
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 09/09/2022
--------------------------------------------------------------------

--
-- comment
--

CREATE OR REPLACE TASK task_load_QUERY_ACCELERATION_HISTORY_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
DECLARE 
l_row_count INTEGER;   


   
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';   
TRUNCATE TABLE   &{l_target_db}.&{l_target_schema}.QUERY_ACCELERATION_HISTORY_stg;
      
l_row_count := (SELECT COUNT(*)
                   FROM snowflake.account_usage.QUERY_ACCELERATION_HISTORY
                   WHERE convert_timezone('Europe/London',end_time)::timestamp_ntz >= 
                   to_timestamp(
                     select ifnull( dateadd( hour, -4, max( end_time ) ), '2020-01-01' ) as last_control_dt
                       from &{l_target_db}.&{l_target_schema}.QUERY_ACCELERATION_HISTORY) ); 


insert into
    &{l_target_db}.&{l_target_schema}.QUERY_ACCELERATION_HISTORY_stg
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,START_TIME,END_TIME,CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME,
DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS) 
    select
         '&{l_hub_org_name}'                as organization_name
        , UPPER('&{l_account_name}')        as account_name
        , current_region()                  as region_name
        , (convert_timezone('Europe/London', start_time) )::timestamp_tz
        , (convert_timezone('Europe/London', end_time) )::timestamp_tz
        ,s.credits_used
        ,s.warehouse_id
        ,s.warehouse_name
        ,'QUERY_ACCELERATION_HISTORY'
        ,:l_row_count
        ,(convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz
    from
        snowflake.account_usage.QUERY_ACCELERATION_HISTORY s
    where
        convert_timezone('Europe/London',s.end_time)::timestamp_ntz >= to_timestamp(
          select ifnull( dateadd( hour, -4, max( end_time ) ), '2020-01-01' ) as last_control_dt
            from &{l_target_db}.&{l_target_schema}.QUERY_ACCELERATION_HISTORY)
      and convert_timezone('Europe/London',s.end_time)::timestamp_ntz < convert_timezone('Europe/London',current_timestamp())::date;
END;
$$;


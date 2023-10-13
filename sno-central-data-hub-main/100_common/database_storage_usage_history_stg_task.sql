--------------------------------------------------------------------
--  Purpose: create task task_load_database_storage_usage_history_stg after --  --task_load_automatic_clustering_history_stg
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 09/09/2022
--------------------------------------------------------------------

--
-- comment
--


CREATE  OR REPLACE TASK task_load_database_storage_usage_history_stg
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
DECLARE 
l_row_count INTEGER;   
   
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/London';   
TRUNCATE TABLE &{l_target_db}.&{l_target_schema}.database_storage_usage_history_stg;
       
l_row_count := (SELECT COUNT(*)
                   FROM snowflake.account_usage.database_storage_usage_history
                   WHERE convert_timezone('Europe/London',usage_date)::timestamp_ntz >= to_timestamp(
          select ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) as last_control_dt
            from &{l_target_db}.&{l_target_schema}.database_storage_usage_history));
INSERT into
    &{l_target_db}.&{l_target_schema}.database_storage_usage_history_stg
(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,
USAGE_DATE,DATABASE_ID,DATABASE_NAME,DELETED,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES
,DW_FILE_NAME,DW_FILE_ROW_NO,DW_LOAD_TS) 
    select
         '&{l_hub_org_name}'                as organization_name
        , UPPER('&{l_account_name}')        as account_name
        , current_region()                  as region_name
        ,convert_timezone('Europe/London',s.usage_date)::timestamp_ntz
        ,s.database_id
        ,s.database_name
        ,s.deleted
        ,s.average_database_bytes
        ,s.average_failsafe_bytes
        ,'database_storage_usage_history'
        ,:l_row_count
        ,(convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz
    from
        snowflake.account_usage.database_storage_usage_history s
    where
        convert_timezone('Europe/London',s.usage_date)::timestamp_ntz >= to_timestamp(
          select ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) as last_control_dt
            from &{l_target_db}.&{l_target_schema}.database_storage_usage_history) 
        and convert_timezone('Europe/London',s.usage_date)::timestamp_ntz < convert_timezone('Europe/London',current_timestamp())::date;

END;
$$;





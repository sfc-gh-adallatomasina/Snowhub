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

CREATE OR REPLACE TASK task_load_warehouse_metering_history_hst
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_warehouse_metering_history_stg
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London'; 
insert into
    &{l_target_db}.&{l_target_schema}.warehouse_metering_history
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
         sha1_binary( concat( s.account_name
                             ,'|', s.organization_name
                             ,'|', s.region_name
                             ,'|', to_char( NVL(s.warehouse_name, 'UNKNOWN') )
                             ,'|', to_char( s.warehouse_id )
                             ,'|', to_char(convert_timezone('Europe/London',s.start_time), 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    )               as dw_event_shk
        ,s.*
    from
        &{l_target_db}.&{l_target_schema}.warehouse_metering_history_stg s
        where
    s.start_time >= to_timestamp(
      
          select
        ifnull( dateadd( hour, -4, max( start_time ) ), '2020-01-01' ) as last_control_dt
    from
        &{l_target_db}.&{l_target_schema}.warehouse_metering_history)
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_event_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_event_shk
    ,s.organization_name
    ,s.account_name
    ,s.region_name
        , (convert_timezone('Europe/London', start_time) )::timestamp_tz
    ,(convert_timezone('Europe/London', end_time) )::timestamp_tz
    ,s.warehouse_id
    ,s.warehouse_name
    ,s.credits_used
    ,s.credits_used_compute
    ,s.credits_used_cloud_services
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,(convert_timezone('Europe/London', current_timestamp()) )::timestamp_tz    as dw_load_ts
from
    l_deduped s
where
    s.dw_event_shk not in
    (
        select dw_event_shk from &{l_target_db}.&{l_target_schema}.warehouse_metering_history 
       
    )
order by
    start_time  -- physically sort rows by a logical partitioning date
;END;
 $$;



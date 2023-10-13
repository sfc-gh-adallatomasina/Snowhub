--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 29/10/2021 Will Riley  
--------------------------------------------------------------------

--

--
-- permanent history table with retention days
--
create table if not exists &{l_target_db}_DATA_REP.&{l_target_schema}.QUERY_ACCELERATION_HISTORY
(
     dw_event_shk                   binary( 20 )        null
    ,organization_name              varchar( 250 )      null
    ,account_name                   varchar( 250 )      null
    ,region_name                    varchar( 250 )      null
    ,start_time                     timestamp_tz       null
    ,end_time                       timestamp_tz       null
    ,credits_used                   number(38,9)       null
    ,warehouse_id                    number(38,0)          null
    ,warehouse_name                  varchar( 250 )      null
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 1
;

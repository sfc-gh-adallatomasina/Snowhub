--------------------------------------------------------------------
--  Purpose: create tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
-- 29/10/2021 
--------------------------------------------------------------------


--
-- permanent history table with retention days
--
create table if not exists &{l_target_db}_DATA_REP.&{l_target_schema}.data_transfer_history
(
     dw_event_shk                   binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_tz       not null
    ,end_time                       timestamp_tz       not null
    ,source_cloud                   varchar( 250 )
    ,source_region                  varchar( 250 )
    ,target_cloud                   varchar( 250 )
    ,target_region                  varchar( 250 )
    ,bytes_transferred              number
    ,transfer_type                  varchar( 250 )
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 1
;



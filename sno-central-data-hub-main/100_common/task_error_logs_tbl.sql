--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------------- ----------------------------------
--  dd/mm/yy
--  09/08/23 Nareesh Komuravelly  Initial version to capture task failures
--------------------------------------------------------------------

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create table if not exists  &{l_target_db}.&{l_target_schema}.task_error_logs
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,name                           varchar             not null
    ,database_name                  varchar             not null
    ,schema_name                    varchar             not null
    ,query_id                       varchar             null
    ,state                          varchar             null
    ,error_message                  varchar             null
    ,QUERY_START_TIME               timestamp_tz        null
    ,COMPLETED_TIME                 timestamp_tz        null
    ,query_text                     varchar             null
    ,dw_load_ts                     timestamp_tz       not null
)
data_retention_time_in_days = 90
;
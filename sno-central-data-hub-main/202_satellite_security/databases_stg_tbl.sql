--  Purpose: create stage table for databases
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	stage table for raw data collection from account_usage
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TRANSIENT TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.databases_stg
(
   ORGANIZATION_NAME               varchar( 250 )      NOT NULL
  ,ACCOUNT_NAME                    varchar( 250 )      NOT NULL
  ,REGION_NAME                     varchar( 250 )      NOT NULL
  ,DATABASE_ID                     number              NOT NULL
  ,DATABASE_NAME                   varchar      NOT NULL
  ,DATABASE_OWNER                  varchar      NULL
  ,IS_TRANSIENT                    varchar      NULL
  ,COMMENT                         varchar      NULL
  ,CREATED                         TIMESTAMP_LTZ       NULL
  ,LAST_ALTERED                    TIMESTAMP_LTZ       NULL
  ,DELETED                         TIMESTAMP_LTZ       NULL
  ,RETENTION_TIME                  number              NULL
  ,DW_FILE_NAME                    varchar( 250 )      NOT NULL
  ,DW_LOAD_TS                      TIMESTAMP_LTZ        NOT NULL
)data_retention_time_in_days = 1;



--  Purpose: create history table for databases history
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	history table will hold unique records based upon hash key
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.databases_history
(
   DW_EVENT_SHK                    binary( 20 )        NOT NULL    
  ,ORGANIZATION_NAME               varchar( 250 )      NOT NULL
  ,ACCOUNT_NAME                    varchar( 250 )      NOT NULL
  ,REGION_NAME                     varchar( 250 )      NOT NULL
  ,DATABASE_ID                     number              NULL
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
)data_retention_time_in_days = 90;

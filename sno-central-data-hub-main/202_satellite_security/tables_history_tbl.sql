--  Purpose: create table for tables view
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 85/05/22 	sayali phadtare 	table for raw data collection from account_usage

--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.tables_history(
   ORGANIZATION_NAME              varchar( 250 )       NOT NULL
  ,ACCOUNT_NAME                   varchar( 250 )       NOT NULL
  ,REGION_NAME                    varchar( 250 )       NOT NULL
  ,TABLE_ID                       number               NULL
  ,TABLE_NAME                     varchar       NULL
  ,TABLE_SCHEMA_ID                NUMBER        NULL
  ,TABLE_SCHEMA                   varchar       NULL
  ,TABLE_CATALOG_ID               number        NULL
  ,TABLE_CATALOG                  varchar       NULL
  ,TABLE_OWNER                    varchar       NULL
  ,TABLE_TYPE                     varchar       NULL
  ,IS_TRANSIENT                   varchar       NULL
  ,CLUSTERING_KEY                 varchar       NULL
  ,ROW_COUNT                      number        NULL
  ,BYTES                          number        NULL
  ,RETENTION_TIME                 number        NULL
  ,SELF_REFERENCING_COLUMN_NAME   varchar       NULL
  ,REFERENCE_GENERATION	          varchar       NULL
  ,USER_DEFINED_TYPE_CATALOG      varchar       NULL
  ,USER_DEFINED_TYPE_SCHEMA	      varchar       NULL
  ,USER_DEFINED_TYPE_NAME         varchar       NULL
  ,IS_INSERTABLE_INTO             varchar       NULL
  ,IS_TYPED                       varchar       NULL
  ,COMMIT_ACTION                  varchar       NULL
  ,CREATED                        TIMESTAMP_LTZ NULL
  ,LAST_ALTERED                   TIMESTAMP_LTZ NULL
  ,DELETED                        TIMESTAMP_LTZ NULL
  ,AUTO_CLUSTERING_ON             varchar       NULL
  ,COMMENT                        varchar       NULL
  ,DW_FILE_NAME                   varchar       NOT NULL
  ,dw_file_row_no                 number        NOT null
  ,DW_LOAD_TS                     TIMESTAMP_LTZ NOT NULL
)data_retention_time_in_days = 90;



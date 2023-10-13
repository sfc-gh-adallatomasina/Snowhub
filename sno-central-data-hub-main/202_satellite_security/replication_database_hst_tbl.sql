--  Purpose: history table for account_parameter
--
--  Revision History:
--  Date     Engineer                Description
--  -------- ------------- --------------------------------------------------------------------
-- 26/05/2023	sayali phadtare 	account_parameter 
-- 27/06/2023   Nareesh Komuravelly Synced table def with source
------------------------------------------------------------------------------------------------ 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


CREATE TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.replication_databases_history
(
DW_EVENT_SHK                     binary( 20 )    NOT NULL,    
ORGANIZATION_NAME                VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)    NOT  NULL,
REGION_NAME                      VARCHAR(250)    NOT NULL,
REGION_GROUP                     VARCHAR         NOT NULL,
SNOWFLAKE_REGION                 VARCHAR         NOT NULL,
REPLICATION_ACCOUNT_NAME         VARCHAR         NOT NULL,
DATABASE_NAME                    VARCHAR         NOT NULL,
COMMENT                          VARCHAR         NULL,
CREATED                          TIMESTAMP_LTZ   NOT NULL,
IS_PRIMARY                       BOOLEAN,
PRIMARY                          VARCHAR         NOT NULL,
REPLICATION_ALLOWED_TO_ACCOUNTS  VARCHAR         NULL,
FAILOVER_ALLOWED_TO_ACCOUNTS     VARCHAR         NULL,
EFFECTIVE_FROM                   TIMESTAMP_LTZ   NOT NULL,
EFFECTIVE_TO                     TIMESTAMP_LTZ   NULL
) data_retention_time_in_days = 90;
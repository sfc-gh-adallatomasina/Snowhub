--  Purpose: stage table for managed_accounts
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	managed_accounts 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


CREATE TRANSIENT TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.managed_accounts_stg
(
ORGANIZATION_NAME                VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)    NOT  NULL,
REGION_NAME                      VARCHAR(250)    NOT NULL,
READER_ACC_NAME                  VARCHAR         NOT NULL,
CLOUD                            VARCHAR         NOT NULL,
MANAGED_REGION                   VARCHAR         NOT NULL,
LOCATOR                          VARCHAR         NOT NULL,  
CREATED_ON                       TIMESTAMP_LTZ   NOT NULL,
URL                              VARCHAR         NOT NULL,
IS_READER                        BOOLEAN,
COMMENT                          VARCHAR         NULL,  
REGION_GROUP                     VARCHAR         NULL,
DW_LOAD_TS                       TIMESTAMP_LTZ  NOT NULL
)data_retention_time_in_days = 1;

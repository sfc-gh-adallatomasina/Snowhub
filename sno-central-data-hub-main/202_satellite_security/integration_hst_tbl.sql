--  Purpose: history table for account_integration 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	account_integration 
----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

CREATE TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY (
DW_EVENT_SHK                    binary( 20 )    NOT NULL,   
ORGANIZATION_NAME               VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                    VARCHAR(250)    NOT NULL,
REGION_NAME                     VARCHAR(250)    NOT NULL,
INTEGRATION_NAME                VARCHAR    NOT NULL, 
INTEGRATION_TYPE                VARCHAR   NOT NULL,
INTEGRATION_CATEGORY            VARCHAR   NOT NULL,
ENABLED                         BOOLEAN,
COMMENT                         VARCHAR    NULL,  
CREATED                         TIMESTAMP_LTZ    NOT NULL,
EFFECTIVE_FROM                  TIMESTAMP_LTZ    NOT NULL,
EFFECTIVE_TO                    TIMESTAMP_LTZ 
) data_retention_time_in_days = 90;


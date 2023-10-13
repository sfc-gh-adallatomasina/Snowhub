--  Purpose: stage table for account_parameter
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	account_parameter 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

CREATE TRANSIENT TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_STG
(
ORGANIZATION_NAME                VARCHAR(250)   NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)   NOT  NULL,
REGION_NAME                      VARCHAR(250)   NOT NULL,
PARAMETER_NAME                   VARCHAR        NOT NULL,
PARAMETER_VALUE                  VARCHAR        NULL,
PARAMETER_DEFAULT_VALUE          VARCHAR        NULL,
PARAMETER_LEVEL                  VARCHAR        NULL,
DW_LOAD_TS                       TIMESTAMP_LTZ  NOT NULL
) data_retention_time_in_days = 1;
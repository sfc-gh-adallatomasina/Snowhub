--  Purpose: stage table for desc user 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	user_detail 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

CREATE TRANSIENT TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG (
ORGANIZATION_NAME               VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                    VARCHAR(250)    NOT NULL,
REGION_NAME                     VARCHAR(250)    NOT NULL,
USER_NAME                       VARCHAR   NOT NULL,
PROPERTY_NAME                   VARCHAR   NOT NULL,
PROPERTY_VALUE                  VARCHAR   NULL,
DW_LOAD_TS                      TIMESTAMP_LTZ    NOT NULL
) data_retention_time_in_days = 1;
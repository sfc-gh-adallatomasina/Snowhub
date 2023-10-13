--  Purpose: stage table for show_network_policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	show_network_policy 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

CREATE TRANSIENT TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.network_policy_stg (
ORGANIZATION_NAME               VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                    VARCHAR(250)    NOT NULL,
REGION_NAME                     VARCHAR(250)    NOT NULL,
CREATED_ON                      TIMESTAMP_LTZ    NOT NULL, 
POLICY_NAME                     VARCHAR   NOT NULL,
ENTRIES_IN_ALLOWED_IP_LIST      NUMBER   ,
ENTRIES_IN_BLOCKED_IP_LIST      NUMBER   ,
DW_LOAD_TS                     TIMESTAMP_LTZ    NOT NULL
) data_retention_time_in_days = 1;
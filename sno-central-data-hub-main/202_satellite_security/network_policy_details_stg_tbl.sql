--  Purpose: create stage table for network policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 02/01/23  sayali phadtare  stage table for raw data collection from network policy in the account

--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

CREATE TRANSIENT TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.network_policy_details_stg (
ORGANIZATION_NAME               VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                    VARCHAR(250)    NOT NULL,
REGION_NAME                     VARCHAR(250)    NOT NULL,
POLICY_NAME                     VARCHAR   NOT NULL,
POLICY_TYPE                     VARCHAR   NOT NULL,
POLICY_VALUE                    VARCHAR   NOT NULL,
DW_LOAD_TS                     TIMESTAMP_LTZ    NOT NULL
) data_retention_time_in_days = 1;

--  Purpose: create history table for network policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 09/01/23  sayali phadtare  history table will hold unique records based upon hash key

--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.network_policy_details_history (
DW_EVENT_SHK                    binary( 20 )    NOT NULL,    
ORGANIZATION_NAME               VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                    VARCHAR(250)    NOT NULL,
REGION_NAME                     VARCHAR(250)    NOT NULL,
POLICY_NAME                     VARCHAR   NOT NULL,
POLICY_TYPE                     VARCHAR   NOT NULL,
POLICY_VALUE                    VARCHAR   NOT NULL,
EFFECTIVE_FROM                  TIMESTAMP_LTZ    NOT NULL,
EFFECTIVE_TO                    TIMESTAMP_LTZ    
) data_retention_time_in_days = 90;
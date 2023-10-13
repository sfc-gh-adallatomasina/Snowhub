--  Purpose: create stage table for users
--
--  Revision History:
--  Date     Engineer               Description
--  -------- -------------------    ----------------------------------
-- 08/06/23  Nareesh Komuravelly 	stage table for raw data collection from account_usage
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

CREATE TRANSIENT TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_STG 
(
ORGANIZATION_NAME                VARCHAR(250)   NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)   NOT  NULL,
REGION_NAME                      VARCHAR(250)   NOT NULL,
USER_NAME                        VARCHAR        NOT NULL,
PARAMETER_NAME                   VARCHAR        NOT NULL,
PARAMETER_VALUE                  VARCHAR        NULL,
PARAMETER_DEFAULT_VALUE          VARCHAR        NULL,
PARAMETER_LEVEL                  VARCHAR        NULL,
DW_LOAD_TS                       TIMESTAMP_LTZ  NOT NULL

)data_retention_time_in_days = 1;
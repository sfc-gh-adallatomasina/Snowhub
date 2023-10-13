--  Purpose: create history table for users history
--
--  Revision History:
--  Date     Engineer               Description
--  -------- -------------------    ----------------------------------
-- 08/06/23  Nareesh Komuravelly 	history table will hold unique records based upon hash key
--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
create TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_HISTORY
(
DW_EVENT_SHK                     binary( 20 )    NOT NULL,     
ORGANIZATION_NAME                VARCHAR(250)    NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)    NOT  NULL,
REGION_NAME                      VARCHAR(250)    NOT NULL,
USER_NAME                        VARCHAR        NOT NULL,
PARAMETER_NAME                   VARCHAR        NOT NULL,
PARAMETER_VALUE                  VARCHAR        NULL,
PARAMETER_DEFAULT_VALUE          VARCHAR        NULL,
PARAMETER_LEVEL                  VARCHAR        NULL,
EFFECTIVE_FROM                   TIMESTAMP_LTZ   NOT NULL,
EFFECTIVE_TO                     TIMESTAMP_LTZ   NULL

)data_retention_time_in_days = 90;
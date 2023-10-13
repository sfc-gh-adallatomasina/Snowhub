--  Purpose: create history table for rest_event_history 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 20/12/22 	sayali phadtare 	history table will hold unique records based upon hash key
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.rest_event_history_history
(
   DW_EVENT_SHK                  binary( 20 )              NOT NULL,  
   ORGANIZATION_NAME             varchar( 250 )            NOT NULL,	
   ACCOUNT_NAME                  varchar( 250 )            NOT NULL,	
   REGION_NAME                   varchar( 250 )            NOT NULL,
   EVENT_TIMESTAMP               timestamp_ltz   NULL,
   EVENT_ID                      number          NULL,
   EVENT_TYPE                    VARCHAR         NULL,
   ENDPOINT                      VARCHAR         NULL,
   METHOD                        VARCHAR         NULL,
   STATUS                        VARCHAR         NULL,
   ERROR_CODE                    number          NULL,
   DETAILS                       VARCHAR         NULL,
   CLIENT_IP                     VARCHAR         NULL,
   ACTOR_NAME                    varchar         NULL,
   ACTOR_DOMAIN                  varchar         NULL,
   RESOURCE_NAME                 varchar         NULL,
   RESOURCE_DOMAIN               varchar         NULL,
   DW_FILE_NAME                  varchar         NOT NULL,	
   DW_LOAD_TS                    TIMESTAMP_LTZ   NOT NULL	
)data_retention_time_in_days = 90;

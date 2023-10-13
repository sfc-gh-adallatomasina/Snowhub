--  Purpose: create stage table for login history
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	stage table for raw data collection from account_usage
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- transient staging table with no retention days
---

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TRANSIENT TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.login_history_stg(
ORGANIZATION_NAME               	VARCHAR(250)	 NOT NULL,
ACCOUNT_NAME               			VARCHAR(250)	 NOT NULL,
REGION_NAME      	            	VARCHAR(250)	 NOT NULL,
EVENT_ID	                        NUMBER           NOT NULL,		
EVENT_TIMESTAMP	                    TIMESTAMP_LTZ    NULL,		
EVENT_TYPE	                        VARCHAR     NULL,		
USER_NAME	                        VARCHAR     NULL,		
CLIENT_IP	                        VARCHAR     NULL,		
REPORTED_CLIENT_TYPE	            VARCHAR     NULL,		
REPORTED_CLIENT_VERSION	            VARCHAR     NULL,		
FIRST_AUTHENTICATION_FACTOR	        VARCHAR     NULL,		
SECOND_AUTHENTICATION_FACTOR	    VARCHAR     NULL,		
IS_SUCCESS	                        VARCHAR     NULL,		
ERROR_CODE	                        NUMBER      NULL,		
ERROR_MESSAGE	                    VARCHAR     NULL,		
RELATED_EVENT_ID	                NUMBER      NULL,		
CONNECTION	                        VARCHAR     NULL,		
DW_FILE_NAME               			VARCHAR(250)	 NOT NULL,
DW_LOAD_TS		                    TIMESTAMP_LTZ     NOT NULL	
)data_retention_time_in_days = 1;




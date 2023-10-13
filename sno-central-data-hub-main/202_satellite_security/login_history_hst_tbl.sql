--  Purpose: create history table for login history
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	history table will hold unique records based upon hash key
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- permanent table with retention days
--

CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.login_history_history(
DW_EVENT_SHK                        binary( 20 )     NOT NULL,
ORGANIZATION_NAME               	VARCHAR(250)	 NOT NULL,
ACCOUNT_NAME               			VARCHAR(250)	 NOT NULL,
REGION_NAME      	            	VARCHAR(250)	 NOT NULL,
EVENT_ID	                        NUMBER           NULL,		
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
)data_retention_time_in_days = 90;

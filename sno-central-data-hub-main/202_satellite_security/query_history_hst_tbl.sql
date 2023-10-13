--  Purpose: create history table for login history
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22   sayali phadtare     stage table for raw data collection from account_usage
-- 22/12/22   sayali phadtare     removed row_count
-- 07/08/23   Nareesh Komuravelly Fixed string data types
--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.query_history_history(
DW_EVENT_SHK                                binary( 20 )        NOT NULL,
ORGANIZATION_NAME    						VARCHAR(250)		NOT NULL,
ACCOUNT_NAME         						VARCHAR(250)		NOT NULL,
REGION_NAME          						VARCHAR(250)		NOT NULL,
QUERY_ID	         						VARCHAR     		NOT NULL,		
QUERY_TEXT	         						VARCHAR             NULL,		
DATABASE_ID	         						NUMBER   			NULL,		
DATABASE_NAME	     						VARCHAR             NULL,		
SCHEMA_ID	         						NUMBER   			NULL,		
SCHEMA_NAME	         						VARCHAR             NULL,		
QUERY_TYPE	         						VARCHAR             NULL,		
SESSION_ID	         						NUMBER   			NULL,		
USER_NAME	         						VARCHAR             NULL,		
ROLE_NAME	         						VARCHAR             NULL,		
WAREHOUSE_ID	     						NUMBER   			NULL,		
WAREHOUSE_NAME	     						VARCHAR             NULL,		
WAREHOUSE_SIZE	     						VARCHAR             NULL,		
WAREHOUSE_TYPE	    						VARCHAR             NULL,		
CLUSTER_NUMBER	     						NUMBER   			NULL,		
QUERY_TAG	         						VARCHAR             NULL,		
EXECUTION_STATUS	 						VARCHAR             NULL,		
ERROR_CODE	         						VARCHAR             NULL,		
ERROR_MESSAGE	     						VARCHAR             NULL,		
START_TIME	         						TIMESTAMP_LTZ		NULL,		
END_TIME	         						TIMESTAMP_LTZ		NULL,		
TOTAL_ELAPSED_TIME	 						NUMBER   			NULL,		
BYTES_SCANNED	     						NUMBER  			NULL,		
PERCENTAGE_SCANNED_FROM_CACHE				FLOAT  				NULL,		
BYTES_WRITTEN	                			NUMBER   			NULL,		
BYTES_WRITTEN_TO_RESULT	        			NUMBER   			NULL,		
BYTES_READ_FROM_RESULT	        			NUMBER  			NULL,		
ROWS_PRODUCED	                			NUMBER   			NULL,		
ROWS_INSERTED	                			NUMBER   			NULL,		
ROWS_UPDATED                   				NUMBER   			NULL,		
ROWS_DELETED                 				NUMBER   			NULL,		
ROWS_UNLOADED	                			NUMBER   			NULL,		
BYTES_DELETED	                			NUMBER   			NULL,		
PARTITIONS_SCANNED	           				NUMBER   			NULL,		
PARTITIONS_TOTAL	            			NUMBER   			NULL,		
BYTES_SPILLED_TO_LOCAL_STORAGE	         	NUMBER   			NULL,		
BYTES_SPILLED_TO_REMOTE_STORAGE	         	NUMBER   			NULL,		
BYTES_SENT_OVER_THE_NETWORK	             	NUMBER   			NULL,		
COMPILATION_TIME	                     	NUMBER   			NULL,		
EXECUTION_TIME	                         	NUMBER   			NULL,		
QUEUED_PROVISIONING_TIME	             	NUMBER   			NULL,		
QUEUED_REPAIR_TIME	                     	NUMBER   			NULL,		
QUEUED_OVERLOAD_TIME	                	NUMBER   			NULL,		
TRANSACTION_BLOCKED_TIME	             	NUMBER   			NULL,		
OUTBOUND_DATA_TRANSFER_CLOUD	         	VARCHAR             NULL,		
OUTBOUND_DATA_TRANSFER_REGION	         	VARCHAR             NULL,		
OUTBOUND_DATA_TRANSFER_BYTES	         	NUMBER   			NULL,		
INBOUND_DATA_TRANSFER_CLOUD	            	VARCHAR             NULL,		
INBOUND_DATA_TRANSFER_REGION	         	VARCHAR             NULL,		
INBOUND_DATA_TRANSFER_BYTES              	NUMBER   			NULL,		
LIST_EXTERNAL_FILES_TIME	             	NUMBER   			NULL,		
CREDITS_USED_CLOUD_SERVICES	             	FLOAT          		NULL,		
RELEASE_VERSION	                         	VARCHAR             NULL,		
EXTERNAL_FUNCTION_TOTAL_INVOCATIONS	        NUMBER   			NULL,		
EXTERNAL_FUNCTION_TOTAL_SENT_ROWS	        NUMBER   			NULL,		
EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS	    NUMBER   			NULL,		
EXTERNAL_FUNCTION_TOTAL_SENT_BYTES	        NUMBER   			NULL,		
EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES	    NUMBER   			NULL,		
QUERY_LOAD_PERCENT	                        NUMBER   			NULL,		
IS_CLIENT_GENERATED_STATEMENT	            BOOLEAN    			NULL,		
QUERY_ACCELERATION_BYTES_SCANNED	        NUMBER   			NULL,		
QUERY_ACCELERATION_PARTITIONS_SCANNED	    NUMBER   			NULL,		
QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR	NUMBER				NULL,		
DW_FILE_NAME                                VARCHAR(250)		NOT NULL,
DW_LOAD_TS		                            TIMESTAMP_LTZ		NOT NULL
) data_retention_time_in_days = 90;



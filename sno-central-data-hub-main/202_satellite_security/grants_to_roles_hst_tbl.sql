--  Purpose: create history table for grants to roles history
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare   history table will hold unique records based upon hash key
-- 22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- permanent table with retention days
--

CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.grants_to_roles_history(
DW_EVENT_SHK                        binary( 20 )    NOT NULL,
ORGANIZATION_NAME               	VARCHAR(250)	NOT NULL,
ACCOUNT_NAME               			VARCHAR(250)	NOT NULL,
REGION_NAME      	            	VARCHAR(250)	NOT NULL,
CREATED_ON	                        TIMESTAMP_LTZ   NULL,		
MODIFIED_ON	                        TIMESTAMP_LTZ   NULL,		
PRIVILEGE	                        VARCHAR    NULL,		
GRANTED_ON	                        VARCHAR    NULL,		
NAME	                            VARCHAR    NULL,		
TABLE_CATALOG	                    VARCHAR    NULL,		
TABLE_SCHEMA	                    VARCHAR    NULL,		
GRANTED_TO	                        VARCHAR    NULL,		
GRANTEE_NAME	                    VARCHAR    NULL,		
GRANT_OPTION	                    BOOLEAN    NULL,		
GRANTED_BY	                        VARCHAR    NULL,		
DELETED_ON	                        TIMESTAMP_LTZ   NULL,		
GRANTED_BY_ROLE_TYPE	            VARCHAR(250)    NULL,		
DW_FILE_NAME               			VARCHAR(250)    NOT NULL,
DW_LOAD_TS		                    TIMESTAMP_LTZ   NOT NULL	
)data_retention_time_in_days = 90;

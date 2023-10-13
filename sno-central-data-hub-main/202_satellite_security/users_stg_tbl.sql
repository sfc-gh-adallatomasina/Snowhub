--  Purpose: create stage table for users
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare 	stage table for raw data collection from account_usage
--22/12/2022   sayali phadtare   removed row_count
--------------------------------------------------------------------

--
-- transient staging table with no retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
CREATE TRANSIENT TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.users_stg(
ORGANIZATION_NAME               	VARCHAR(250)	NOT null	,
ACCOUNT_NAME               			VARCHAR(250)	NOT null	,
REGION_NAME      	            	VARCHAR(250)	NOT null	,
NAME	                            VARCHAR     	null,
CREATED_ON	                        TIMESTAMP_LTZ	null,
DELETED_ON	                        TIMESTAMP_LTZ	null,
LOGIN_NAME	                        VARCHAR     	null,
DISPLAY_NAME                     	VARCHAR     	null,
FIRST_NAME	                        VARCHAR     	null,
LAST_NAME	                        VARCHAR     	null,
EMAIL	                            VARCHAR     	null,
MUST_CHANGE_PASSWORD	            BOOLEAN	        NULL,
HAS_PASSWORD	                    BOOLEAN	        NULL,
COMMENT	                            VARCHAR     	null,
DISABLED	                        VARIANT	        NULL,
SNOWFLAKE_LOCK	                    VARIANT	        NULL,
DEFAULT_WAREHOUSE	                VARCHAR     	null,
DEFAULT_NAMESPACE	                VARCHAR     	null,
DEFAULT_ROLE	                    VARCHAR     	null,
EXT_AUTHN_DUO	                    VARIANT	        NULL,
EXT_AUTHN_UID	                    VARCHAR     	null,
BYPASS_MFA_UNTIL	                TIMESTAMP_LTZ	null,
LAST_SUCCESS_LOGIN                 	TIMESTAMP_LTZ	null,
EXPIRES_AT	                        TIMESTAMP_LTZ	null,
LOCKED_UNTIL_TIME	                TIMESTAMP_LTZ	null,
HAS_RSA_PUBLIC_KEY	                BOOLEAN	        NULL,
PASSWORD_LAST_SET_TIME	            TIMESTAMP_LTZ	null,
OWNER	                            VARCHAR     	null,
DEFAULT_SECONDARY_ROLE	            VARCHAR	        null,
DW_FILE_NAME               			VARCHAR(250)	NOT NULL,
DW_LOAD_TS                    		TIMESTAMP_LTZ	NOT NULL	
) data_retention_time_in_days = 1;


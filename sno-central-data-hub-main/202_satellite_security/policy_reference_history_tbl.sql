--  Purpose: history table for POLICY_REFERENCES 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	POLICY_REFERENCES 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.policy_reference_history (
    ORGANIZATION_NAME               varchar( 250 )    NOT NULL,
    ACCOUNT_NAME                    varchar( 250 )    NOT NULL,
    REGION_NAME                     varchar( 250 )    NOT NULL,
	POLICY_DB 						VARCHAR           NULL,
	POLICY_SCHEMA 					VARCHAR           NULL,
	POLICY_ID 						NUMBER(38,0)      NULL,
	POLICY_NAME 					VARCHAR           NULL,
	POLICY_KIND 					VARCHAR           NULL,
	REF_DATABASE_NAME 				VARCHAR           NULL,
	REF_SCHEMA_NAME 				VARCHAR           NULL,
	REF_ENTITY_NAME 				VARCHAR           NULL,
	REF_ENTITY_DOMAIN 				VARCHAR           NULL,
	REF_COLUMN_NAME 				VARCHAR           NULL,
	REF_ARG_COLUMN_NAMES 			VARCHAR           NULL,
	TAG_DATABASE 					VARCHAR           NULL,
	TAG_SCHEMA 						VARCHAR           NULL,
	TAG_NAME 						VARCHAR           NULL,
	POLICY_STATUS 					VARCHAR           NULL,
    DW_FILE_NAME                    VARCHAR           NULL,
    DW_FILE_ROW_NO                  number            NULL,
    DW_LOAD_TS                      TIMESTAMP_LTZ     NOT NULL
)data_retention_time_in_days = 90;

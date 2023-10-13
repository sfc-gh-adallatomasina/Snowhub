--  Purpose: create history table for password policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 08/05/23  sayali phadtare  history table will hold unique records based upon hash key

--------------------------------------------------------------------

--
-- permanent table with retention days
--

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

CREATE TABLE IF NOT EXISTS &{l_target_db}.&{l_sec_schema}.PASSWORD_POLICY_HISTORY(
    DW_EVENT_SHK                                        BINARY( 20 )  NOT NULL,
    ORGANIZATION_NAME	                                VARCHAR(250)  NOT NULL,
    ACCOUNT_NAME	                                    VARCHAR(250)  NOT NULL,
    REGION_NAME                                      	VARCHAR(250)  NOT NULL,
	ID                                                  NUMBER(38,0)  NOT NULL,
	NAME                                                VARCHAR       NOT NULL,
	SCHEMA_ID                                           NUMBER(38,0)  NOT NULL,
	SCHEMA                                              VARCHAR       NOT NULL,
	DATABASE_ID                                         NUMBER(38,0)  NOT NULL,
	DATABASE                                            VARCHAR       NOT NULL,
	OWNER                                               VARCHAR,
	OWNER_ROLE_TYPE                                     VARCHAR,
	PASSWORD_MIN_LENGTH                                 NUMBER(38,0) NOT NULL,
	PASSWORD_MAX_LENGTH                                 NUMBER(38,0) NOT NULL,
	PASSWORD_MIN_UPPER_CASE_CHARS                       NUMBER(38,0) NOT NULL,
	PASSWORD_MIN_LOWER_CASE_CHARS                       NUMBER(38,0) NOT NULL,
	PASSWORD_MIN_NUMERIC_CHARS                          NUMBER(38,0) NOT NULL,
	PASSWORD_MIN_SPECIAL_CHARS                          NUMBER(38,0) NOT NULL,
	PASSWORD_MAX_AGE_DAYS                               NUMBER(38,0) NOT NULL,
	PASSWORD_MAX_RETRIES                                NUMBER(38,0) NOT NULL,
	PASSWORD_LOCKOUT_TIME_MINS                          NUMBER(38,0) NOT NULL,
	COMMENT                                             VARCHAR      NOT NULL,
	CREATED                                             TIMESTAMP_LTZ(9) NOT NULL,
	LAST_ALTERED                                        TIMESTAMP_LTZ(9),
	DELETED                                             TIMESTAMP_LTZ(9),
    DW_FILE_NAME               			                VARCHAR(250)	 NOT NULL,
    DW_LOAD_TS                                          TIMESTAMP_LTZ    NOT NULL
) data_retention_time_in_days = 90;




--  Purpose: create history task
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 09/06/23  Nareesh Komuravelly 	Initial version
--------------------------------------------------------------------
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

create or replace procedure &{l_target_db}.&{l_sec_schema}.sp_users_load()
 RETURNS VARCHAR 
 LANGUAGE SQL
 EXECUTE AS CALLER
AS
$$
DECLARE
  v_user VARCHAR;
  out string default ''; 
  sql_stmt varchar;
BEGIN
  --Truncate Stage table
  TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.USERS_STG;

  --out := out || CHAR(10) || 'Starting - Load Stage table; ';
  --Load stage table
  INSERT INTO &{l_target_db}.&{l_sec_schema}.users_stg(ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,NAME,CREATED_ON,DELETED_ON,LOGIN_NAME,DISPLAY_NAME
       ,FIRST_NAME,LAST_NAME,EMAIL,MUST_CHANGE_PASSWORD,HAS_PASSWORD,COMMENT,DISABLED,SNOWFLAKE_LOCK,DEFAULT_WAREHOUSE,DEFAULT_NAMESPACE
       ,DEFAULT_ROLE,EXT_AUTHN_DUO,EXT_AUTHN_UID,BYPASS_MFA_UNTIL,LAST_SUCCESS_LOGIN,EXPIRES_AT,LOCKED_UNTIL_TIME,HAS_RSA_PUBLIC_KEY
       ,PASSWORD_LAST_SET_TIME,OWNER,DEFAULT_SECONDARY_ROLE,DW_FILE_NAME,DW_LOAD_TS) 
  SELECT ($ORG_NAME)                as ORGANIZATION_NAME ,
         ($ACCOUNT_NAME)            as ACCOUNT_NAME ,
         ($REGION)                  as REGION_NAME,
         s.NAME,
         s.CREATED_ON, 
         s.DELETED_ON,
         s.LOGIN_NAME,
         s.DISPLAY_NAME,
         s.FIRST_NAME,
         s.LAST_NAME,
         s.EMAIL,
         s.MUST_CHANGE_PASSWORD,
         s.HAS_PASSWORD,
         s.COMMENT,
         s.DISABLED,
         s.SNOWFLAKE_LOCK,
         s.DEFAULT_WAREHOUSE,
         s.DEFAULT_NAMESPACE,
         s.DEFAULT_ROLE,
         s.EXT_AUTHN_DUO,
         s.EXT_AUTHN_UID,
         s.BYPASS_MFA_UNTIL,
         s.LAST_SUCCESS_LOGIN,
         s.EXPIRES_AT,
         s.LOCKED_UNTIL_TIME,
         s.HAS_RSA_PUBLIC_KEY,
         s.PASSWORD_LAST_SET_TIME,
         s.OWNER,
         s.DEFAULT_SECONDARY_ROLE,
         'USERS',
         current_timestamp()
  FROM snowflake.account_usage.users s;

  out := out || CHAR(10) || 'Stage table load complete; ';
  out := out || CHAR(10) || 'Inserting new records into History; ';
  
  --Load new records into History table
  INSERT INTO &{l_target_db}.&{l_sec_schema}.USERS_HISTORY(DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, NAME, CREATED_ON, DELETED_ON
      , LOGIN_NAME, DISPLAY_NAME, FIRST_NAME, LAST_NAME, EMAIL, MUST_CHANGE_PASSWORD, HAS_PASSWORD, COMMENT, DISABLED, SNOWFLAKE_LOCK, DEFAULT_WAREHOUSE
      , DEFAULT_NAMESPACE, DEFAULT_ROLE, EXT_AUTHN_DUO, EXT_AUTHN_UID, BYPASS_MFA_UNTIL, LAST_SUCCESS_LOGIN, EXPIRES_AT, LOCKED_UNTIL_TIME
      , HAS_RSA_PUBLIC_KEY, PASSWORD_LAST_SET_TIME, OWNER, DEFAULT_SECONDARY_ROLE, EFFECTIVE_FROM)
  SELECT sha1_binary( concat( NAME,'|', CREATED_ON,'|', NVL(DELETED_ON,'2021-01-01'),'|', LOGIN_NAME,'|', NVL(DISPLAY_NAME,''),'|', NVL(FIRST_NAME,'')
         ,'|', NVL(LAST_NAME,'') ,'|', NVL(EMAIL,''),'|', NVL(MUST_CHANGE_PASSWORD,TRUE),'|', NVL(HAS_PASSWORD,TRUE),'|', NVL(COMMENT,'')
         ,'|', NVL(DISABLED,''),'|', NVL(SNOWFLAKE_LOCK,''),'|', NVL(DEFAULT_WAREHOUSE,''),'|', NVL(DEFAULT_NAMESPACE,''),'|', NVL(DEFAULT_ROLE,'') 
         ,'|', NVL(EXT_AUTHN_DUO,''),'|', NVL(EXT_AUTHN_UID,''),'|', NVL(BYPASS_MFA_UNTIL,'2021-01-01'),'|', NVL(LAST_SUCCESS_LOGIN,'2021-01-01')
         ,'|', NVL(EXPIRES_AT,'2021-01-01'),'|', NVL(LOCKED_UNTIL_TIME,'2021-01-01'),'|', NVL(HAS_RSA_PUBLIC_KEY,TRUE)
         ,'|', NVL(PASSWORD_LAST_SET_TIME,'2021-01-01'),'|', NVL(OWNER,''),'|', NVL(DEFAULT_SECONDARY_ROLE,'')  )) as DW_EVENT_SHK
      , ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, NAME, CREATED_ON, DELETED_ON, LOGIN_NAME, DISPLAY_NAME, FIRST_NAME, LAST_NAME, EMAIL
      , MUST_CHANGE_PASSWORD, HAS_PASSWORD, COMMENT, DISABLED, SNOWFLAKE_LOCK, DEFAULT_WAREHOUSE, DEFAULT_NAMESPACE, DEFAULT_ROLE, EXT_AUTHN_DUO
      , EXT_AUTHN_UID, BYPASS_MFA_UNTIL, LAST_SUCCESS_LOGIN, EXPIRES_AT, LOCKED_UNTIL_TIME, HAS_RSA_PUBLIC_KEY, PASSWORD_LAST_SET_TIME, OWNER
      , DEFAULT_SECONDARY_ROLE, DW_LOAD_TS
  FROM &{l_target_db}.&{l_sec_schema}.USERS_STG
  WHERE (NAME, CREATED_ON, NVL(DELETED_ON,'2021-01-01'), LOGIN_NAME, NVL(DISPLAY_NAME,''), NVL(FIRST_NAME,''), NVL(LAST_NAME,''), NVL(EMAIL,'')
           , NVL(MUST_CHANGE_PASSWORD,TRUE), NVL(HAS_PASSWORD,TRUE), NVL(COMMENT,''), NVL(DISABLED,''), NVL(SNOWFLAKE_LOCK,''), NVL(DEFAULT_WAREHOUSE,'')
           , NVL(DEFAULT_NAMESPACE,''), NVL(DEFAULT_ROLE,''), NVL(EXT_AUTHN_DUO,''), NVL(EXT_AUTHN_UID,''), NVL(BYPASS_MFA_UNTIL,'2021-01-01')
           , NVL(LAST_SUCCESS_LOGIN,'2021-01-01'), NVL(EXPIRES_AT,'2021-01-01'), NVL(LOCKED_UNTIL_TIME,'2021-01-01'), NVL(HAS_RSA_PUBLIC_KEY,TRUE)
           , NVL(PASSWORD_LAST_SET_TIME,'2021-01-01'), NVL(OWNER,''), NVL(DEFAULT_SECONDARY_ROLE,''))  
    NOT IN (SELECT NAME, CREATED_ON, NVL(DELETED_ON,'2021-01-01'), LOGIN_NAME, NVL(DISPLAY_NAME,''), NVL(FIRST_NAME,''), NVL(LAST_NAME,''), NVL(EMAIL,'')
           , NVL(MUST_CHANGE_PASSWORD,TRUE), NVL(HAS_PASSWORD,TRUE), NVL(COMMENT,''), NVL(DISABLED,''), NVL(SNOWFLAKE_LOCK,''), NVL(DEFAULT_WAREHOUSE,'')
           , NVL(DEFAULT_NAMESPACE,''), NVL(DEFAULT_ROLE,''), NVL(EXT_AUTHN_DUO,''), NVL(EXT_AUTHN_UID,''), NVL(BYPASS_MFA_UNTIL,'2021-01-01')
           , NVL(LAST_SUCCESS_LOGIN,'2021-01-01'), NVL(EXPIRES_AT,'2021-01-01'), NVL(LOCKED_UNTIL_TIME,'2021-01-01'), NVL(HAS_RSA_PUBLIC_KEY,TRUE)
           , NVL(PASSWORD_LAST_SET_TIME,'2021-01-01'), NVL(OWNER,''), NVL(DEFAULT_SECONDARY_ROLE,'')
            FROM &{l_target_db}.&{l_sec_schema}.USERS_HISTORY
            WHERE EFFECTIVE_TO IS NULL
            )
  ;

  out := out || CHAR(10) || 'Ending old records into History; ';
  --Update old records in History table
  UPDATE &{l_target_db}.&{l_sec_schema}.USERS_HISTORY
    SET  EFFECTIVE_TO = CURRENT_TIMESTAMP()
    WHERE (NAME, CREATED_ON, NVL(DELETED_ON,'2021-01-01'), LOGIN_NAME, NVL(DISPLAY_NAME,''), NVL(FIRST_NAME,''), NVL(LAST_NAME,''), NVL(EMAIL,'')
           , NVL(MUST_CHANGE_PASSWORD,TRUE), NVL(HAS_PASSWORD,TRUE), NVL(COMMENT,''), NVL(DISABLED,''), NVL(SNOWFLAKE_LOCK,''), NVL(DEFAULT_WAREHOUSE,'')
           , NVL(DEFAULT_NAMESPACE,''), NVL(DEFAULT_ROLE,''), NVL(EXT_AUTHN_DUO,''), NVL(EXT_AUTHN_UID,''), NVL(BYPASS_MFA_UNTIL,'2021-01-01')
           , NVL(LAST_SUCCESS_LOGIN,'2021-01-01'), NVL(EXPIRES_AT,'2021-01-01'), NVL(LOCKED_UNTIL_TIME,'2021-01-01'), NVL(HAS_RSA_PUBLIC_KEY,TRUE)
           , NVL(PASSWORD_LAST_SET_TIME,'2021-01-01'), NVL(OWNER,''), NVL(DEFAULT_SECONDARY_ROLE,'')) 
      NOT IN (SELECT NAME, CREATED_ON, NVL(DELETED_ON,'2021-01-01'), LOGIN_NAME, NVL(DISPLAY_NAME,''), NVL(FIRST_NAME,''), NVL(LAST_NAME,''), NVL(EMAIL,'')
               , NVL(MUST_CHANGE_PASSWORD,TRUE), NVL(HAS_PASSWORD,TRUE), NVL(COMMENT,''), NVL(DISABLED,''), NVL(SNOWFLAKE_LOCK,''), NVL(DEFAULT_WAREHOUSE,'')
               , NVL(DEFAULT_NAMESPACE,''), NVL(DEFAULT_ROLE,''), NVL(EXT_AUTHN_DUO,''), NVL(EXT_AUTHN_UID,''), NVL(BYPASS_MFA_UNTIL,'2021-01-01')
               , NVL(LAST_SUCCESS_LOGIN,'2021-01-01'), NVL(EXPIRES_AT,'2021-01-01'), NVL(LOCKED_UNTIL_TIME,'2021-01-01'), NVL(HAS_RSA_PUBLIC_KEY,TRUE)
               , NVL(PASSWORD_LAST_SET_TIME,'2021-01-01'), NVL(OWNER,''), NVL(DEFAULT_SECONDARY_ROLE,'') 
              FROM &{l_target_db}.&{l_sec_schema}.USERS_STG
             )
      AND EFFECTIVE_TO IS NULL
  ;

  RETURN 'PASS';
exception
    when statement_error then
      return object_construct('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate,
                            'ACTUAL MESSAGE', out);
    when other then
      return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate,
                            'ACTUAL MESSAGE', out);
END;
$$
;
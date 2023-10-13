--  Purpose: procedure for show_network_policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	aprocedure to load data into stage and history table  

----------------------------------------------------------------------------------------------------------- 
use role ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_user_parameters_load() 
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

  TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_STG;
  
  out := out || CHAR(10) || 'Starting - List users; ';

  sql_stmt := 'SHOW USERS';
  EXECUTE IMMEDIATE :sql_stmt ;
  
  --SHOW USERS IN ACCOUNT;
  LET c1_users cursor for SELECT "name" NAME FROM TABLE ( RESULT_SCAN ( last_query_id()));

  --LET c1_users cursor for SELECT DISTINCT NAME FROM &{l_target_db}.&{l_sec_schema}.USERS_HISTORY WHERE DELETED_ON IS NULL AND NAME <> 'SNOWFLAKE';
  
  for rec in c1_users do
    out := out || CHAR(10) || 'Inside for loop -  Describe Users; ';
    v_user := rec.NAME;

    IF (:v_user != 'SNOWFLAKE') THEN
      sql_stmt := 'SHOW PARAMETERS FOR USER "'|| :v_user || '"';
      EXECUTE IMMEDIATE :sql_stmt ;  

      out := out || CHAR(10) || 'Inserting data into Stage table; ';
      INSERT INTO &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_STG (ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME
        , PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL, DW_LOAD_TS) 
      SELECT  ($ORG_NAME), ($ACCOUNT_NAME), ($REGION), :v_user, "key", "value", "default", "level", current_timestamp()
      FROM TABLE ( RESULT_SCAN ( last_query_id()));
    END IF;

  END FOR;

  out := out || CHAR(10) || 'Stage table load complete; ';

  out := out || CHAR(10) || 'Ending old records into History; ';                                                        
  UPDATE &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_HISTORY SET EFFECTIVE_TO = CURRENT_TIMESTAMP() 
  WHERE (USER_NAME, PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'')) 
    NOT IN (SELECT USER_NAME, PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'') FROM &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_STG)
    AND EFFECTIVE_TO IS NULL;

  out := out || CHAR(10) || 'Inserting new records into History; ';
  INSERT INTO &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_HISTORY(DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME
      , USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL, EFFECTIVE_FROM)
    SELECT sha1_binary( concat( USER_NAME,'|', PARAMETER_NAME,'|', NVL(PARAMETER_VALUE,''),'|', NVL(PARAMETER_DEFAULT_VALUE,''),'|', NVL(PARAMETER_LEVEL,'')))
         , ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL, CURRENT_TIMESTAMP()
  FROM &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_STG
  WHERE (USER_NAME, PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'')) 
    NOT IN (SELECT USER_NAME, PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'') 
              FROM &{l_target_db}.&{l_sec_schema}.USER_PARAMETERS_HISTORY WHERE EFFECTIVE_TO IS NULL);



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
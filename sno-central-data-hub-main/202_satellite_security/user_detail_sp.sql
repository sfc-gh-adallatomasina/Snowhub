--  Purpose: procedure for user_detail
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

create or replace procedure  &{l_target_db}.&{l_sec_schema}.user_details_load()
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

  TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG;
  
  out := out || CHAR(10) || 'Starting - List Users; ';
  
  --SHOW USERS IN ACCOUNT;
  LET c1_users cursor for SELECT DISTINCT NAME FROM &{l_target_db}.&{l_sec_schema}.USERS_HISTORY WHERE DELETED_ON IS NULL AND NAME <> 'SNOWFLAKE';
  
  for rec in c1_users do
    out := out || CHAR(10) || 'Inside for loop -  Describe Users; ';
    v_user := rec.NAME;

    sql_stmt := 'DESCRIBE USER "'|| :v_user || '"';
    EXECUTE IMMEDIATE :sql_stmt ;  
    
    out := out || CHAR(10) || 'Inserting data into Stage table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG (ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PROPERTY_NAME, PROPERTY_VALUE, DW_LOAD_TS) 
    SELECT  ($ORG_NAME), ($ACCOUNT_NAME), ($REGION), :v_user, "property", "value", current_timestamp()
    FROM TABLE ( RESULT_SCAN ( last_query_id()))
    ;

  END FOR;

  out := out || CHAR(10) || 'Stage table load complete; ';

  out := out || CHAR(10) || 'Inserting new records into History using MERGE; ';

  MERGE INTO &{l_target_db}.&{l_sec_schema}.USER_DETAILS_HISTORY t
    USING &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG s ON t.USER_NAME = s.USER_NAME AND t.PROPERTY_NAME = s.PROPERTY_NAME AND t.EFFECTIVE_TO IS NULL
  WHEN NOT MATCHED
    THEN INSERT ( DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME
                , USER_NAME, PROPERTY_NAME, PROPERTY_VALUE, EFFECTIVE_FROM)
         VALUES ( sha1_binary( concat( s.USER_NAME,'|', s.PROPERTY_NAME) ), s.ORGANIZATION_NAME, s.ACCOUNT_NAME
                , s.REGION_NAME, s.USER_NAME, s.PROPERTY_NAME, s.PROPERTY_VALUE, s.DW_LOAD_TS)
  WHEN MATCHED AND NVL(t.PROPERTY_VALUE, ' ') <> NVL(s.PROPERTY_VALUE, ' ')
    THEN UPDATE SET t.EFFECTIVE_TO = CURRENT_TIMESTAMP();

  out := out || CHAR(10) || 'Inserting new records into History; ';
  INSERT INTO &{l_target_db}.&{l_sec_schema}.USER_DETAILS_HISTORY (DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PROPERTY_NAME, PROPERTY_VALUE, EFFECTIVE_FROM)
  SELECT sha1_binary(concat( USER_NAME,'|', PROPERTY_NAME)), ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME
       , PROPERTY_NAME, PROPERTY_VALUE, DW_LOAD_TS
  FROM &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG
  WHERE (USER_NAME, PROPERTY_NAME) NOT IN (SELECT USER_NAME, PROPERTY_NAME FROM &{l_target_db}.&{l_sec_schema}.USER_DETAILS_HISTORY    WHERE EFFECTIVE_TO IS NULL);

  out := out || CHAR(10) || 'End records in target that are deleted from source; ';  
  UPDATE &{l_target_db}.&{l_sec_schema}.USER_DETAILS_HISTORY SET EFFECTIVE_TO = CURRENT_TIMESTAMP()
    WHERE EFFECTIVE_TO IS NULL 
      AND (USER_NAME, PROPERTY_TYPE) NOT IN (SELECT USER_NAME, PROPERTY_TYPE FROM &{l_target_db}.&{l_sec_schema}.USER_DETAILS_STG );
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

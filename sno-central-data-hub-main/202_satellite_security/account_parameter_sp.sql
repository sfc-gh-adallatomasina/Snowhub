--  Purpose: procedure for account_parameter
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	aprocedure to load data into stage and history table  

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

create or replace procedure  &{l_target_db}.&{l_sec_schema}.SP_ACCOUNT_PARAMETERS_LOAD()
  returns varchar
  language sql
  EXECUTE AS CALLER
  as  
  $$
  DECLARE
  out string default ''; 
  BEGIN
    out := out || CHAR(10) || 'Begin - Truncate stage table; ';
    --Truncate stage table 
    TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_STG ;

    out := out || CHAR(10) || 'Load data into Stage table; ';

    show parameters in account;
    SELECT "key","value","default","level" from table(RESULT_SCAN(LAST_QUERY_ID()));

    --Insert into stage table     
    INSERT INTO &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_STG 
    SELECT ($ORG_NAME), ($ACCOUNT_NAME), ($REGION), "key", "value", "default", "level", current_timestamp()
      FROM table(RESULT_SCAN(LAST_QUERY_ID()));

    out := out || CHAR(10) || 'Logically end old records in history table; ';
    --End old records in history table                                                  
    UPDATE &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_HISTORY SET EFFECTIVE_TO = CURRENT_TIMESTAMP() 
      WHERE (PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'')) 
          NOT IN (SELECT PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'') FROM &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_STG)
        AND EFFECTIVE_TO IS NULL;

    CALL SYSTEM$WAIT(5);    
    out := out || CHAR(10) || 'Insert new records in history table; ';
    
    --Insert new records into history table
    INSERT INTO &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_HISTORY(DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME
        , PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL, EFFECTIVE_FROM)
      SELECT sha1_binary( concat( PARAMETER_NAME,'|', NVL(PARAMETER_VALUE,''),'|', NVL(PARAMETER_DEFAULT_VALUE,''),'|', NVL(PARAMETER_LEVEL,'')))
           , ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL, CURRENT_TIMESTAMP()
      FROM &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_STG
      WHERE (PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'')) 
        NOT IN (SELECT PARAMETER_NAME, NVL(PARAMETER_VALUE,''), NVL(PARAMETER_DEFAULT_VALUE,''), NVL(PARAMETER_LEVEL,'') 
                FROM &{l_target_db}.&{l_sec_schema}.ACCOUNT_PARAMETERS_HISTORY WHERE EFFECTIVE_TO IS NULL);


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
  $$;
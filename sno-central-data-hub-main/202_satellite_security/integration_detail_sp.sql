--  Purpose: procedure for integration_detail
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	procedure to load data into stage and history table  

----------------------------------------------------------------------------------------------------------- 
use role ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_integration_details_load()
  returns varchar
  language sql
  EXECUTE AS CALLER
  as  
  $$
  DECLARE
  v_integration VARCHAR;
  out string default ''; 
  sql_stmt varchar;

  BEGIN
    
    TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_STG ;
    

    show integrations;
    LET c1_integrations cursor for SELECT "name" from table(RESULT_SCAN(LAST_QUERY_ID()));
     
    for rec in c1_integrations do

    v_integration := rec."name";

    sql_stmt := 'DESCRIBE integration "'|| :v_integration || '"';
    EXECUTE IMMEDIATE :sql_stmt ; 

    INSERT INTO &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_STG 
    select ($ORG_NAME) as ORGANIZATION_NAME
    ,($ACCOUNT_NAME) as ACCOUNT_NAME
    ,($REGION) as REGION_NAME
    ,:v_integration as INTEGRATION_NAME
    ,"property" as PROPERTY_NAME
    ,"property_value" as PROPERTY_VALUE 
    ,current_timestamp() as DW_LOAD_TS
    from table(RESULT_SCAN(LAST_QUERY_ID()))
    ;

   END FOR;

    out := out || CHAR(10) || 'End old records in history table; ';
    UPDATE &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_HISTORY SET EFFECTIVE_TO = CURRENT_TIMESTAMP()
      WHERE (INTEGRATION_NAME,PROPERTY_NAME,NVL(PROPERTY_VALUE,''))
          NOT IN (SELECT INTEGRATION_NAME,PROPERTY_NAME,NVL(PROPERTY_VALUE,'') FROM &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_STG)
        AND EFFECTIVE_TO IS NULL;

    out := out || CHAR(10) || 'Insert new records into history table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_HISTORY(DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME
         , REGION_NAME, INTEGRATION_NAME, PROPERTY_NAME, PROPERTY_VALUE, EFFECTIVE_FROM)
      SELECT sha1_binary( concat( s.INTEGRATION_NAME,'|',s.PROPERTY_NAME,'|', NVL(s.PROPERTY_VALUE,'')))
         , s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.INTEGRATION_NAME,s.PROPERTY_NAME,s.PROPERTY_VALUE, current_timestamp()
      FROM &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_STG s
      WHERE (s.INTEGRATION_NAME, s.PROPERTY_NAME, NVL(s.PROPERTY_VALUE,''))
          NOT IN (SELECT INTEGRATION_NAME,PROPERTY_NAME,NVL(PROPERTY_VALUE,'') FROM &{l_target_db}.&{l_sec_schema}.INTEGRATION_DETAILS_HISTORY
                  WHERE EFFECTIVE_TO IS NULL);

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

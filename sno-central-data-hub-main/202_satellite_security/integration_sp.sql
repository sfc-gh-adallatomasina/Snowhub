use role ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_integrations_load()
  returns varchar
  language sql
  EXECUTE AS CALLER
  as  
  $$
  DECLARE
  out string default ''; 
  BEGIN
    TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG;

    out := out || CHAR(10) || 'Starting - List Integrations; ';
    SHOW INTEGRATIONS;

    out := out || CHAR(10) || 'Inserting data into Stage table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG
    SELECT ($ORG_NAME), ($ACCOUNT_NAME), ($REGION), "name", "type", "category", "enabled", "comment", "created_on", current_timestamp()
     FROM TABLE ( RESULT_SCAN ( last_query_id()));
    out := out || CHAR(10) || 'Stage table load complete; ';

    out := out || CHAR(10) || 'End old records in history table; ';
    UPDATE  &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY  SET EFFECTIVE_TO=current_timestamp()
     WHERE (INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED) 
         NOT IN (SELECT INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG)
       AND EFFECTIVE_TO IS NULL;

    out := out || CHAR(10) || 'Insert new records into history table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY (DW_EVENT_SHK ,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,INTEGRATION_NAME
          , INTEGRATION_TYPE,INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED, EFFECTIVE_FROM)
      SELECT sha1_binary( concat( INTEGRATION_NAME,'|', INTEGRATION_TYPE,'|', INTEGRATION_CATEGORY))
           , ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, INTEGRATION_TYPE
           , INTEGRATION_CATEGORY, ENABLED, COMMENT, CREATED, DW_LOAD_TS
      FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG 
      WHERE (INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, NVL(COMMENT,''), CREATED) 
         NOT IN (SELECT INTEGRATION_NAME, INTEGRATION_TYPE, INTEGRATION_CATEGORY, ENABLED, NVL(COMMENT,''), CREATED FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY
                 WHERE EFFECTIVE_TO IS NULL);
/*     
    MERGE INTO &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY t 
     USING &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG AS s
      on t.INTEGRATION_NAME=s.INTEGRATION_NAME and t.INTEGRATION_TYPE=s.INTEGRATION_TYPE 
      and t.INTEGRATION_CATEGORY=s.INTEGRATION_CATEGORY and t.EFFECTIVE_TO IS NULL
    
    WHEN MATCHED 
      AND (t.ENABLED <> s.ENABLED or nvl(t.CREATED,'') <> nvl(s.CREATED,''))
       THEN UPDATE SET t.EFFECTIVE_TO=current_timestamp()

    WHEN NOT MATCHED
      THEN INSERT ( DW_EVENT_SHK ,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,INTEGRATION_NAME, INTEGRATION_TYPE,INTEGRATION_CATEGORY,
                     ENABLED,COMMENT,CREATED,EFFECTIVE_FROM)                    
      VALUES (sha1_binary( concat( s.INTEGRATION_NAME,'|', s.INTEGRATION_TYPE,'|', s.INTEGRATION_CATEGORY)),
        s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.INTEGRATION_NAME, s.INTEGRATION_TYPE,s.INTEGRATION_CATEGORY,
                     s.ENABLED,s.COMMENT,s.CREATED,s.DW_LOAD_TS);
      
    INSERT INTO &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,INTEGRATION_NAME
       , INTEGRATION_TYPE,INTEGRATION_CATEGORY,ENABLED,COMMENT,CREATED,EFFECTIVE_FROM)       
     SELECT sha1_binary( concat( INTEGRATION_NAME,'|', INTEGRATION_TYPE,'|', INTEGRATION_CATEGORY))
          , ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,INTEGRATION_NAME,INTEGRATION_TYPE,INTEGRATION_CATEGORY,ENABLED,COMMENT,CREATED,DW_LOAD_TS
      FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG 
      WHERE (INTEGRATION_NAME,INTEGRATION_TYPE,INTEGRATION_CATEGORY) 
         NOT IN (SELECT INTEGRATION_NAME,INTEGRATION_TYPE,INTEGRATION_CATEGORY FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY
                 WHERE EFFECTIVE_TO IS NULL)
    ;   

     UPDATE  &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_HISTORY  SET EFFECTIVE_TO=current_timestamp()
     WHERE (INTEGRATION_NAME,INTEGRATION_TYPE,INTEGRATION_CATEGORY) NOT IN 
               (SELECT INTEGRATION_NAME,INTEGRATION_TYPE,INTEGRATION_CATEGORY FROM &{l_target_db}.&{l_sec_schema}.INTEGRATIONS_STG)
       AND EFFECTIVE_TO IS NULL;
*/

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
    



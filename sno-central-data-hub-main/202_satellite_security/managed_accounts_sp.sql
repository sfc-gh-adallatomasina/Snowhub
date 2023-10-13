--  Purpose: procedure for managed_accounts
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	  procedure to load data into stage and history table  
-- 04/07/2023 Nareesh Komuravelly re-written the program
----------------------------------------------------------------------------------------------------------- 
use role ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';


CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_managed_accounts_load()
  returns varchar
  language sql
  EXECUTE AS CALLER 
  as  
  $$

  DECLARE
  out string default ''; 

  BEGIN
    DELETE FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG;
    out := out || CHAR(10) || 'Starting - Show Managed Accounts; ';
    SHOW MANAGED ACCOUNTS;
    
    out := out || CHAR(10) || 'Inserting data into Stage table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG
    select ($ORG_NAME) as ORGANIZATION_NAME
      , ($ACCOUNT_NAME) as ACCOUNT_NAME
      , ($REGION) as REGION_NAME
      , "name" as READER_ACC_NAME
      , "cloud" as CLOUD
      , "region" as MANAGED_REGION
      , "locator" as LOCATOR 
      , "created_on" as CREATED_ON
      , "url" as URL
      , "is_reader" as IS_READER
      , "comment" as COMMENT 
      , "region_group" as REGION_GROUP
      , current_timestamp() as DW_LOAD_TS
    FROM TABLE ( RESULT_SCAN ( last_query_id())) ;


    out := out || CHAR(10) || 'End old records in history table; ';
    UPDATE  &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY SET EFFECTIVE_TO=current_timestamp()
      WHERE (READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, NVL(COMMENT,''), REGION_GROUP) 
          NOT IN (SELECT READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, NVL(COMMENT,''), REGION_GROUP FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG)
        AND EFFECTIVE_TO IS NULL;


    out := out || CHAR(10) || 'Inserting data into History table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY(DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,READER_ACC_NAME
         , CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP, EFFECTIVE_FROM)       
    SELECT sha1_binary(s.LOCATOR),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.READER_ACC_NAME,s.CLOUD,s.MANAGED_REGION,s.LOCATOR
         , s.CREATED_ON, s.URL, s.IS_READER, s.COMMENT, s.REGION_GROUP, s.DW_LOAD_TS
      FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG s 
      WHERE ( s.READER_ACC_NAME,s.CLOUD,s.MANAGED_REGION,s.LOCATOR, s.CREATED_ON, s.URL, s.IS_READER, NVL(s.COMMENT,''), s.REGION_GROUP )
          NOT IN (SELECT READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, NVL(COMMENT,''), REGION_GROUP
                    FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY WHERE EFFECTIVE_TO IS NULL);

  /*  
    MERGE INTO &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY t 
     USING &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG AS s
      on t.LOCATOR=s.LOCATOR and t.EFFECTIVE_TO IS NULL    
    WHEN MATCHED 
      AND (t.READER_ACC_NAME <> s.READER_ACC_NAME or t.CLOUD <> s.CLOUD or t.MANAGED_REGION <> s.MANAGED_REGION 
           or t.CREATED_ON <> s.CREATED_ON or t.URL <> s.URL or t.IS_READER <> s.IS_READER )
       THEN UPDATE SET t.EFFECTIVE_TO=current_timestamp()
    WHEN NOT MATCHED
      THEN INSERT (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,READER_ACC_NAME,CLOUD,MANAGED_REGION,LOCATOR,CREATED_ON,
      URL,IS_READER,COMMENT,REGION_GROUP,EFFECTIVE_FROM)                    
      VALUES (sha1_binary(s.LOCATOR),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.READER_ACC_NAME,s.CLOUD,s.MANAGED_REGION,s.LOCATOR,s.CREATED_ON,s.URL,s.IS_READER,s.COMMENT,s.REGION_GROUP,s.DW_LOAD_TS);


    INSERT INTO &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY 
       (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,READER_ACC_NAME,
       CLOUD,MANAGED_REGION,LOCATOR,CREATED_ON,URL,IS_READER,COMMENT,REGION_GROUP,EFFECTIVE_FROM)       
    SELECT sha1_binary(s.LOCATOR),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.READER_ACC_NAME,s.CLOUD,s.MANAGED_REGION,s.LOCATOR,s.CREATED_ON,s.URL,s.IS_READER,s.COMMENT,s.REGION_GROUP,s.DW_LOAD_TS
      FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG s 
      WHERE s.LOCATOR NOT IN (SELECT LOCATOR FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY WHERE EFFECTIVE_TO IS NULL);    

    UPDATE  &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_HISTORY SET EFFECTIVE_TO=current_timestamp()
      WHERE (LOCATOR) NOT IN (SELECT LOCATOR FROM &{l_target_db}.&{l_sec_schema}.MANAGED_ACCOUNTS_STG)
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

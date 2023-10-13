--  Purpose: procedure for account_parameter
--
--  Revision History:
--  Date        Engineer             Description
--  -------- ------------- --------------------------------------------------------------------
-- 26/05/2023	  sayali phadtare 	   aprocedure to load data into stage and history table  
-- 27/06/2023   Nareesh Komuravelly   Synced table def with source
------------------------------------------------------------------------------------------------ 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

CREATE OR REPLACE PROCEDURE  &{l_target_db}.&{l_sec_schema}.sp_replication_database_load()
  returns varchar
  language sql
  EXECUTE AS CALLER
  as  
  $$
  DECLARE
  out string default ''; 

  BEGIN
    DELETE FROM &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_STG;

    INSERT INTO &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_STG
      (ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME
      ,COMMENT,CREATED,IS_PRIMARY,PRIMARY,REPLICATION_ALLOWED_TO_ACCOUNTS,FAILOVER_ALLOWED_TO_ACCOUNTS,DW_LOAD_TS)    
    SELECT ($ORG_NAME) 
      , ($ACCOUNT_NAME) 
      , ($REGION) 
      , REGION_GROUP
      , SNOWFLAKE_REGION
      , ACCOUNT_NAME
      , DATABASE_NAME
      , COMMENT
      , CREATED
      , IS_PRIMARY
      , PRIMARY
      , REPLICATION_ALLOWED_TO_ACCOUNTS
      , FAILOVER_ALLOWED_TO_ACCOUNTS
      , current_timestamp() as DW_LOAD_TS
    FROM SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_DATABASES
    ;
    /*
    MERGE INTO &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_HISTORY t 
     USING &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_STG AS s
      on t.REPLICATION_ACCOUNT_NAME=s.REPLICATION_ACCOUNT_NAME and s.REGION_GROUP = t.REGION_GROUP 
        and t.DATABASE_NAME=s.DATABASE_NAME and t.SNOWFLAKE_REGION = s.SNOWFLAKE_REGION
        and t.EFFECTIVE_TO IS NULL    
    WHEN MATCHED 
      AND (NVL(t.COMMENT,'') <> NVL(s.COMMENT,'')  OR t.CREATED_ON <> s.CREATED_ON 
           OR t.IS_PRIMARY <> s.IS_PRIMARY OR t.PRIMARY <> s.PRIMARY
           OR NVL(t.REPLICATION_ALLOWED_TO_ACCOUNTS,'') <> NVL(s.REPLICATION_ALLOWED_TO_ACCOUNTS,'')
           OR NVL(t.FAILOVER_ALLOWED_TO_ACCOUNTS,'') <> NVL(s.FAILOVER_ALLOWED_TO_ACCOUNTS,''))
       THEN UPDATE SET t.EFFECTIVE_TO=current_timestamp()
    WHEN NOT MATCHED
      THEN INSERT (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME
                   ,COMMENT,CREATED_ON,IS_PRIMARY,PRIMARY,REPLICATION_ALLOWED_TO_ACCOUNTS,FAILOVER_ALLOWED_TO_ACCOUNTS,EFFECTIVE_FROM)                    
           VALUES (sha1_binary( concat( s.REPLICATION_ACCOUNT_NAME,'|', s.DATABASE_NAME,'|', s.SNOWFLAKE_REGION,'|', s.REGION_GROUP))
                   ,s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.REGION_GROUP,s.SNOWFLAKE_REGION,s.REPLICATION_ACCOUNT_NAME,s.DATABASE_NAME
                   ,s.COMMENT,s.CREATED_ON,s.IS_PRIMARY,s.PRIMARY,s.REPLICATION_ALLOWED_TO_ACCOUNTS,s.FAILOVER_ALLOWED_TO_ACCOUNTS,s.DW_LOAD_TS);
		*/	   
   UPDATE  &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_HISTORY SET EFFECTIVE_TO=current_timestamp()
      WHERE (REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME,NVL(COMMENT,''),CREATED
             ,IS_PRIMARY,PRIMARY,NVL(REPLICATION_ALLOWED_TO_ACCOUNTS,''),NVL(FAILOVER_ALLOWED_TO_ACCOUNTS,'')) 
        NOT IN (SELECT REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME,NVL(COMMENT,''),CREATED
                      ,IS_PRIMARY,PRIMARY,NVL(REPLICATION_ALLOWED_TO_ACCOUNTS,''),NVL(FAILOVER_ALLOWED_TO_ACCOUNTS,'') 
                FROM &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_STG)
        AND EFFECTIVE_TO IS NULL; 
        
	  INSERT INTO &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_HISTORY 
                (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME
                 ,COMMENT,CREATED,IS_PRIMARY,PRIMARY,REPLICATION_ALLOWED_TO_ACCOUNTS,FAILOVER_ALLOWED_TO_ACCOUNTS,EFFECTIVE_FROM) 	               
      SELECT sha1_binary( concat( s.REPLICATION_ACCOUNT_NAME,'|', s.DATABASE_NAME,'|', s.SNOWFLAKE_REGION,'|', s.REGION_GROUP))
             ,s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.REGION_GROUP,s.SNOWFLAKE_REGION,s.REPLICATION_ACCOUNT_NAME,s.DATABASE_NAME
             ,s.COMMENT,s.CREATED,s.IS_PRIMARY,s.PRIMARY,s.REPLICATION_ALLOWED_TO_ACCOUNTS,s.FAILOVER_ALLOWED_TO_ACCOUNTS,s.DW_LOAD_TS
      FROM   &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_STG s 
      WHERE  (s.REGION_GROUP,s.SNOWFLAKE_REGION,s.REPLICATION_ACCOUNT_NAME,s.DATABASE_NAME,NVL(s.COMMENT,''),s.CREATED
             ,s.IS_PRIMARY,s.PRIMARY,NVL(s.REPLICATION_ALLOWED_TO_ACCOUNTS,''),NVL(s.FAILOVER_ALLOWED_TO_ACCOUNTS,'')) 
         NOT IN (SELECT REGION_GROUP,SNOWFLAKE_REGION,REPLICATION_ACCOUNT_NAME,DATABASE_NAME,NVL(COMMENT,''),CREATED
                       ,IS_PRIMARY,PRIMARY,NVL(REPLICATION_ALLOWED_TO_ACCOUNTS,''),NVL(FAILOVER_ALLOWED_TO_ACCOUNTS,'') 
                 FROM &{l_target_db}.&{l_sec_schema}.REPLICATION_DATABASES_HISTORY WHERE EFFECTIVE_TO IS NULL);    


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
  
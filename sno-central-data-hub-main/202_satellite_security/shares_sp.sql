--  Purpose: procedure for show_shares
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	  A procedure to load data into stage and history table  
-- 04/07/2023 Nareesh Komuravelly Re-written code
----------------------------------------------------------------------------------------------------------- 
use role ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};


SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

create or replace procedure  &{l_target_db}.&{l_sec_schema}.sp_shares_load()
  returns varchar
  language sql
  EXECUTE AS CALLER
  as  
  $$
  DECLARE
  out string default ''; 
  BEGIN
    
    TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.shares_Stg ;

    out := out || CHAR(10) || 'Starting - Show Shares; ';
    show shares;
    --SELECT "created_on","kind","name","database_name","to","owner","comment","listing_global_name" from table(RESULT_SCAN(LAST_QUERY_ID()));
     
    out := out || CHAR(10) || 'Inserting data into Stage table; '; 
    INSERT INTO &{l_target_db}.&{l_sec_schema}.shares_stg 
    select ($ORG_NAME) as ORGANIZATION_NAME
      , ($ACCOUNT_NAME) as ACCOUNT_NAME
      , ($REGION) as REGION_NAME
      , "created_on" as CREATED
      , "kind" as SHARE_TYPE
      , "name" as SHARE_NAME
      , "database_name" as DATABASE_NAME 
      , "to" as SHARE_TO
      , "owner" as OWNER
      , "comment" as COMMENT
      , "listing_global_name" as LISTING_GLOBAL_NAME
      , current_timestamp() as DW_LOAD_TS
    from table(RESULT_SCAN(LAST_QUERY_ID()))
    ;

    out := out || CHAR(10) || 'End old records in history table; ';
    UPDATE &{l_target_db}.&{l_sec_schema}.shares_history SET EFFECTIVE_TO=current_timestamp()
      WHERE (CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, NVL(SHARE_TO,''), NVL(OWNER,''), NVL(COMMENT,''), NVL(LISTING_GLOBAL_NAME,'')) 
          NOT IN (SELECT CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, NVL(SHARE_TO,''), NVL(OWNER,''), NVL(COMMENT,''), NVL(LISTING_GLOBAL_NAME,'') 
                    FROM &{l_target_db}.&{l_sec_schema}.shares_stg)
        AND EFFECTIVE_TO IS NULL
    ;

    out := out || CHAR(10) || 'Inserting data into History table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.shares_history ( DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED, SHARE_TYPE
          , SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME, EFFECTIVE_FROM)    
      SELECT sha1_binary(s.SHARE_NAME), s.ORGANIZATION_NAME, s.ACCOUNT_NAME, s.REGION_NAME, s.CREATED, s.SHARE_TYPE, s.SHARE_NAME, s.DATABASE_NAME
          , s.SHARE_TO, s.OWNER, s.COMMENT, s.LISTING_GLOBAL_NAME, s.DW_LOAD_TS 
      FROM &{l_target_db}.&{l_sec_schema}.shares_stg s 
      WHERE (s.CREATED, s.SHARE_TYPE, s.SHARE_NAME, s.DATABASE_NAME, NVL(s.SHARE_TO,''), NVL(s.OWNER,''), NVL(s.COMMENT,''), NVL(s.LISTING_GLOBAL_NAME,'')) 
          NOT IN (SELECT CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, NVL(SHARE_TO,''), NVL(OWNER,''), NVL(COMMENT,''), NVL(LISTING_GLOBAL_NAME,'') 
                    FROM &{l_target_db}.&{l_sec_schema}.shares_history WHERE EFFECTIVE_TO IS NULL)
    ;

/*
    MERGE INTO &{l_target_db}.&{l_sec_schema}.shares_history t 
     USING &{l_target_db}.&{l_sec_schema}.shares_stg AS s
      on t.SHARE_NAME=s.SHARE_NAME and s.SHARE_TYPE=t.SHARE_TYPE and t.EFFECTIVE_TO IS NULL    
    WHEN MATCHED AND (t.DATABASE_NAME <> s.DATABASE_NAME OR t.CREATED <> s.CREATED OR NVL(t.SHARE_TO,'') <> NVL(s.SHARE_TO,'') 
                      OR NVL(t.OWNER,'') <> NVL(s.OWNER,'') OR NVL(s.LISTING_GLOBAL_NAME,'') <> NVL(t.LISTING_GLOBAL_NAME,''))
      THEN UPDATE SET t.EFFECTIVE_TO=current_timestamp()
    WHEN NOT MATCHED
      THEN INSERT (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED,SHARE_TYPE,SHARE_NAME,DATABASE_NAME
                  ,SHARE_TO,OWNER,COMMENT,LISTING_GLOBAL_NAME,EFFECTIVE_FROM)
           VALUES (sha1_binary(s.SHARE_NAME,'|',s.SHARE_TYPE),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.CREATED,s.SHARE_TYPE,s.SHARE_NAME
                  ,s.DATABASE_NAME,s.SHARE_TO,s.OWNER,s.COMMENT,s.LISTING_GLOBAL_NAME,s.DW_LOAD_TS)
    ;
       
    INSERT INTO &{l_target_db}.&{l_sec_schema}.shares_history 
       (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED,SHARE_TYPE,SHARE_NAME,DATABASE_NAME,SHARE_TO,OWNER,COMMENT,LISTING_GLOBAL_NAME,EFFECTIVE_FROM)    
      SELECT sha1_binary(s.SHARE_NAME)
        , s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.CREATED,s.SHARE_TYPE,s.SHARE_NAME,s.DATABASE_NAME,s.SHARE_TO,s.OWNER,s.COMMENT
        , s.LISTING_GLOBAL_NAME,s.DW_LOAD_TS 
      FROM &{l_target_db}.&{l_sec_schema}.shares_stg s 
      WHERE (s.SHARE_NAME,s.SHARE_TYPE) NOT IN (SELECT SHARE_NAME,SHARE_TYPE FROM &{l_target_db}.&{l_sec_schema}.shares_history WHERE EFFECTIVE_TO IS NULL);

    UPDATE &{l_target_db}.&{l_sec_schema}.shares_history SET EFFECTIVE_TO=current_timestamp()
      WHERE (SHARE_NAME,SHARE_TYPE) NOT IN (SELECT SHARE_NAME,SHARE_TYPE FROM &{l_target_db}.&{l_sec_schema}.shares_stg)
        and EFFECTIVE_TO IS NULL;
*/

    RETURN 'PASS';

  EXCEPTION
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

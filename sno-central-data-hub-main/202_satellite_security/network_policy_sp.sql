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

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_network_policy_load()
  returns varchar
  language sql
  EXECUTE AS CALLER --Stored procedure execution error: Unsupported statement type 'SHOW PARAMETER'.
  as  
  $$
  DECLARE
  out string default ''; 
  BEGIN
    DELETE FROM &{l_target_db}.&{l_sec_schema}.network_policy_stg;

    out := out || CHAR(10) || 'Starting - List Network Policies; ';    
    SHOW NETWORK POLICIES IN ACCOUNT; 
    
    out := out || CHAR(10) || 'Inserting data into Stage table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.network_policy_stg
    SELECT ($ORG_NAME), ($ACCOUNT_NAME), ($REGION)
      , "created_on"
      , "name" 
      , "entries_in_allowed_ip_list" 
      , "entries_in_blocked_ip_list" 
      , current_timestamp() as DW_LOAD_TS   
     FROM TABLE ( RESULT_SCAN ( last_query_id())) ;
    out := out || CHAR(10) || 'Stage table load complete; ';

    out := out || CHAR(10) || 'End old records in history table; ';
    UPDATE &{l_target_db}.&{l_sec_schema}.network_policy_history SET EFFECTIVE_TO=current_timestamp() 
      WHERE (CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST) 
          NOT IN (SELECT CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST FROM &{l_target_db}.&{l_sec_schema}.network_policy_stg ) 
        AND EFFECTIVE_TO IS NULL;

    out := out || CHAR(10) || 'Insert new records into history table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.network_policy_history 
        (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST,EFFECTIVE_FROM)
      SELECT sha1_binary(s.POLICY_NAME),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.CREATED_ON,s.POLICY_NAME
           , s.ENTRIES_IN_ALLOWED_IP_LIST, s.ENTRIES_IN_BLOCKED_IP_LIST, s.DW_LOAD_TS 
      FROM &{l_target_db}.&{l_sec_schema}.network_policy_stg s 
      WHERE (CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST)
         NOT IN (SELECT CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST FROM &{l_target_db}.&{l_sec_schema}.network_policy_history WHERE EFFECTIVE_TO IS NULL);

/*
    MERGE INTO &{l_target_db}.&{l_sec_schema}.network_policy_history t 
     USING &{l_target_db}.&{l_sec_schema}.network_policy_stg AS s
      on t.POLICY_NAME=s.POLICY_NAME and t.EFFECTIVE_TO IS NULL
    
    WHEN MATCHED 
      AND (t.ENTRIES_IN_ALLOWED_IP_LIST<>s.ENTRIES_IN_ALLOWED_IP_LIST or NVL(t.ENTRIES_IN_BLOCKED_IP_LIST,'') <> NVL(s.ENTRIES_IN_BLOCKED_IP_LIST,'') )
       THEN UPDATE SET t.EFFECTIVE_TO=current_timestamp()
    WHEN NOT MATCHED
      THEN INSERT (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST,EFFECTIVE_FROM)          
      VALUES (sha1_binary(s.POLICY_NAME),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.CREATED_ON,s.POLICY_NAME
        , s.ENTRIES_IN_ALLOWED_IP_LIST, s.ENTRIES_IN_BLOCKED_IP_LIST, s.DW_LOAD_TS);
 
    INSERT INTO &{l_target_db}.&{l_sec_schema}.network_policy_history 
        (DW_EVENT_SHK,ORGANIZATION_NAME,ACCOUNT_NAME,REGION_NAME,CREATED_ON,POLICY_NAME,ENTRIES_IN_ALLOWED_IP_LIST,ENTRIES_IN_BLOCKED_IP_LIST,EFFECTIVE_FROM)
      SELECT sha1_binary(s.POLICY_NAME),s.ORGANIZATION_NAME,s.ACCOUNT_NAME,s.REGION_NAME,s.CREATED_ON,s.POLICY_NAME
        , s.ENTRIES_IN_ALLOWED_IP_LIST, s.ENTRIES_IN_BLOCKED_IP_LIST,s.DW_LOAD_TS 
      FROM &{l_target_db}.&{l_sec_schema}.network_policy_stg s 
      WHERE s.POLICY_NAME NOT IN (SELECT POLICY_NAME FROM &{l_target_db}.&{l_sec_schema}.network_policy_history WHERE EFFECTIVE_TO IS NULL);  

    UPDATE &{l_target_db}.&{l_sec_schema}.network_policy_history SET EFFECTIVE_TO=current_timestamp() 
      WHERE (POLICY_NAME) NOT IN (SELECT POLICY_NAME FROM &{l_target_db}.&{l_sec_schema}.network_policy_stg ) AND EFFECTIVE_TO IS NULL;
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

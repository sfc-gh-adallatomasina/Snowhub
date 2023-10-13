--  Purpose: stored procedure to check network policy in accounts
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 02/01/23  sayali phadtare  STORED PROCEDURE TO STORE NETWORK POLICY IN TABLE
-- 06/01/23  sayali phadtare  one ip/network policy
-- 09/01/23  sayali phadtare  access session variable

--------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
use database &{l_target_db};
use schema &{l_target_schema};
SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_sec_schema}.sp_network_policy_details() 
RETURNS VARCHAR 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_policy VARCHAR;
  out string default ''; 
  sql_stmt varchar;
BEGIN

  TRUNCATE TABLE &{l_target_db}.&{l_sec_schema}.network_policy_details_stg;
  
  out := out || CHAR(10) || 'Starting - List Network Policies; ';

  sql_stmt := 'SHOW NETWORK POLICIES IN ACCOUNT';
  EXECUTE IMMEDIATE :sql_stmt ;
  
  --SHOW NETWORK POLICIES IN ACCOUNT;
  LET c1_network_policies cursor for SELECT "name" POLICY FROM TABLE ( RESULT_SCAN ( last_query_id()));
  
  for rec in c1_network_policies do
    out := out || CHAR(10) || 'Inside for loop -  Describe Policies; ';
    v_policy := rec.POLICY;
    DESCRIBE NETWORK POLICY IDENTIFIER(:v_policy);
    
    out := out || CHAR(10) || 'Inserting data into Stage table; ';
    INSERT INTO &{l_target_db}.&{l_sec_schema}.network_policy_details_stg (ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE, DW_LOAD_TS) 
    SELECT ($ORG_NAME), ($ACCOUNT_NAME), ($REGION), :v_policy, "name", f.value, current_timestamp()
    FROM TABLE ( RESULT_SCAN ( last_query_id())), TABLE(SPLIT_TO_TABLE("value",',')) f
    GROUP BY 1,2,3,4,5,6,7;

  END FOR;

  out := out || CHAR(10) || 'Stage table load complete; ';

  out := out || CHAR(10) || 'Ending deleted records into History; ';                                                        
  UPDATE &{l_target_db}.&{l_sec_schema}.network_policy_details_history SET EFFECTIVE_TO = CURRENT_TIMESTAMP() 
    WHERE (POLICY_NAME, POLICY_TYPE, POLICY_VALUE) NOT IN (SELECT POLICY_NAME, POLICY_TYPE, POLICY_VALUE FROM &{l_target_db}.&{l_sec_schema}.network_policy_details_stg)
      AND EFFECTIVE_TO IS NULL;

  out := out || CHAR(10) || 'Inserting new records into History; ';
  INSERT INTO &{l_target_db}.&{l_sec_schema}.network_policy_details_history (DW_EVENT_SHK, ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE, EFFECTIVE_FROM)
  SELECT sha1_binary( concat( POLICY_NAME,'|', POLICY_TYPE,'|', POLICY_VALUE)), ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE, current_timestamp()
  FROM &{l_target_db}.&{l_sec_schema}.network_policy_details_stg
  WHERE (POLICY_NAME, POLICY_TYPE, POLICY_VALUE) NOT IN (SELECT POLICY_NAME, POLICY_TYPE, POLICY_VALUE 
          FROM &{l_target_db}.&{l_sec_schema}.network_policy_details_history WHERE EFFECTIVE_TO IS NULL);



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



 

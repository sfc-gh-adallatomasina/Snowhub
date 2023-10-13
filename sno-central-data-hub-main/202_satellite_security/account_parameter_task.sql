--  Purpose: create task to check for account parameter
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	task to load data 
----------------------------------------------------------------------------------------------------------- 

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_account_parameters
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
AS
EXECUTE IMMEDIATE $$
BEGIN
SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';
CALL &{l_target_db}.&{l_sec_schema}.SP_ACCOUNT_PARAMETERS_LOAD();
END;
$$;

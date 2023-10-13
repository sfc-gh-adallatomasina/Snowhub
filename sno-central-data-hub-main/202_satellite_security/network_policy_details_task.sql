--  Purpose: create task to check for network policy
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 02/01/2023	sayali phadtare 	task to load data 
-- 09/01/23     sayali phadtare     session variable set inside the task

-----------------------------------------------------------------------------------------------------------   
CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.task_load_network_policy_details
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER &{l_target_db}.&{l_target_schema}.task_load_rest_event_history_stg
AS
EXECUTE IMMEDIATE $$
BEGIN
SET ORG_NAME ='&{l_hub_org_name}' ;
SET ACCOUNT_NAME ='&{l_ACCOUNT_NAME}' ;
SET REGION ='&{l_satellite_region}';
CALL &{l_target_db}.&{l_sec_schema}.sp_network_policy_details();
END;
$$;

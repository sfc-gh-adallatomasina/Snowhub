--------------------------------------------------------------------
--  Purpose: Deploy security objects in the satellites envs
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 16/12/22 	sayali phadtare  
--------------------------------------------------------------------

--Run the script using snowsql from the Chargeback directory:
--snowsql -c <env> -r sysadmin -D l_ACCOUNT_NAME=<ACCOUNT_NAME> -f security_monitoring_dep.sql -o output_file=security_monitoring_dep.out
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR=true;
---------------------RUN DDL START-----------------------------
--use role securityadmin;
--ALTER USER "SVC_TF_SF_USER" SET ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR=true;
use role accountadmin;
ALTER ACCOUNT SET ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR=true;

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

-- 1) create stage tables in schema

!print create tables in schema

!source 202_satellite_security/databases_stg_tbl.sql
!source 202_satellite_security/grants_to_roles_stg_tbl.sql
!source 202_satellite_security/login_history_stg_tbl.sql
!source 202_satellite_security/query_history_stg_tbl.sql
!source 202_satellite_security/tables_history_tbl.sql
!source 202_satellite_security/users_stg_tbl.sql
!source 202_satellite_security/rest_event_history_stg_tbl.sql
!source 202_satellite_security/network_policy_details_stg_tbl.sql
!source 202_satellite_security/password_policy_stg_tbl.sql

!source 202_satellite_security/account_parameter_stg_tbl.sql
!source 202_satellite_security/integration_detail_stg_tbl.sql
!source 202_satellite_security/integration_stg_tbl.sql
!source 202_satellite_security/managed_accounts_stg_tbl.sql
!source 202_satellite_security/network_policy_stg_tbl.sql
!source 202_satellite_security/replication_database_stg_tbl.sql
--!source 202_satellite_security/user_details_stg_tbl.sql
!source 202_satellite_security/user_parameters_stg_tbl.sql
!source 202_satellite_security/share_stg_tbl.sql


--2) create history tables in schema

!print create tables in schema

!source 202_satellite_security/databases_hst_tbl.sql
!source 202_satellite_security/grants_to_roles_hst_tbl.sql
!source 202_satellite_security/login_history_hst_tbl.sql
!source 202_satellite_security/query_history_hst_tbl.sql
!source 202_satellite_security/users_hst_tbl.sql
!source 202_satellite_security/rest_event_history_hst_tbl.sql
!source 202_satellite_security/network_policy_details_hst_tbl.sql
!source 202_satellite_security/password_policy_hst_tbl.sql

!source 202_satellite_security/account_parameter_hst_tbl.sql
!source 202_satellite_security/integration_detail_hst_tbl.sql
!source 202_satellite_security/integration_hst_tbl.sql
!source 202_satellite_security/managed_accounts_hst_tbl.sql
!source 202_satellite_security/network_policy_hst_tbl.sql
!source 202_satellite_security/policy_reference_history_tbl.sql
!source 202_satellite_security/replication_database_hst_tbl.sql
--!source 202_satellite_security/user_details_hst_tbl.sql
!source 202_satellite_security/user_parameters_history_tbl.sql
!source 202_satellite_security/share_hst_tbl.sql



-- 2) CREATE OR REPLACE TASKs for SNOWFLAKE->STG ingestion
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_target_schema};;
use warehouse &{l_target_wh};

!print CREATE OR REPLACE TASKs for SNOWFLAKE->STG ingestion

!source 202_satellite_security/databases_stg_task.sql
!source 202_satellite_security/grants_to_roles_stg_task.sql
!source 202_satellite_security/login_history_stg_task.sql
!source 202_satellite_security/query_history_stg_task.sql
!source 202_satellite_security/password_policy_stg_task.sql
!source 202_satellite_security/tables_load_task.sql
--!source 202_satellite_security/network_policy_sp.sql
--!source 202_satellite_security/network_policy_stg_task.sql
!source 202_satellite_security/account_parameter_task.sql
!source 202_satellite_security/policy_reference_load_task.sql
!source 202_satellite_security/replication_database_task.sql

--stored procedures
!source 202_satellite_security/users_hst_sp.sql

-- 3) CREATE OR REPLACE TASKs for stg->history ingestion

!print CREATE OR REPLACE TASKs for stg->history ingestion

!source 202_satellite_security/databases_hst_task.sql
!source 202_satellite_security/grants_to_roles_hst_task.sql
!source 202_satellite_security/login_history_hst_task.sql
!source 202_satellite_security/query_history_hst_task.sql
!source 202_satellite_security/users_hst_task.sql
!source 202_satellite_security/password_policy_hst_task.sql
--!source 202_satellite_security/network_policy_hst_task.sql

ALTER TASK  &{l_target_db}.&{l_target_schema}.task_load_tables RESUME;

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

!source 202_satellite_security/account_parameter_sp.sql
!source 202_satellite_security/replication_database_sp.sql


USE ROLE ACCOUNTADMIN;
use warehouse &{l_target_wh};
use database &{l_target_db};
use schema &{l_target_schema};
!source 202_satellite_security/rest_event_history_stg_task.sql
!source 202_satellite_security/rest_event_history_hst_task.sql

!source 202_satellite_security/network_policy_details_sp.sql
!source 202_satellite_security/network_policy_details_task.sql

--!source 202_satellite_security/user_detail_sp.sql
--!source 202_satellite_security/user_detail_task.sql
!source 202_satellite_security/user_parameters_sp.sql
!source 202_satellite_security/user_parameters_task.sql

!source 202_satellite_security/integration_detail_task.sql
!source 202_satellite_security/integration_detail_sp.sql

!source 202_satellite_security/managed_accounts_task.sql
!source 202_satellite_security/managed_accounts_sp.sql

!source 202_satellite_security/shares_task.sql
!source 202_satellite_security/shares_sp.sql 

!source 202_satellite_security/integration_task.sql
!source 202_satellite_security/integration_sp.sql

!source 202_satellite_security/network_policy_task.sql
!source 202_satellite_security/network_policy_sp.sql


/*USE ROLE ACCOUNTADMIN;
grant operate on task &{l_target_db}.&{l_target_schema}.TASK_LOAD_REST_EVENT_HISTORY_HST to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.TASK_LOAD_REST_EVENT_HISTORY_STG to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.TASK_LOAD_NETWORK_POLICY_DETAILS to role &{l_entity_name}_sysadmin&{l_fr_suffix};
--grant operate on task &{l_target_db}.&{l_target_schema}.TASK_LOAD_USER_DETAILS to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.task_user_parameters to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.TASK_LOAD_INTEGRATION_DETAILS to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.task_load_managed_accounts to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant operate on task &{l_target_db}.&{l_target_schema}.task_load_shares to role &{l_entity_name}_sysadmin&{l_fr_suffix};
*/

use role ACCOUNTADMIN;
use warehouse &{l_target_wh};
use database &{l_target_db};
use schema &{l_target_schema};

/*
ALTER TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_REST_EVENT_HISTORY_HST RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_NETWORK_POLICY_DETAILS RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.task_load_user_parameters RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_INTEGRATION_DETAILS RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.task_load_managed_accounts RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.task_load_shares RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.task_load_integrations RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.task_load_network_policy RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_REPLICATION_DATABASE RESUME;
ALTER TASK &{l_target_db}.&{l_target_schema}.TASK_LOAD_REST_EVENT_HISTORY_STG RESUME;
*/

select system$task_dependents_enable('TASK_LOAD_REST_EVENT_HISTORY_STG');

ALTER TASK TASK_LOAD_REST_EVENT_HISTORY_STG RESUME;





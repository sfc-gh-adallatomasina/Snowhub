--------------------------------------------------------------------
--  Purpose: Deploy objects in the satellites envs
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  08/12/21 Alessandro Dallatomasina   First release
--  24/02/22 Alessandro Dallatomasina   External configuration file added
--  08/09/22 Alessandro Dallatomasina   Landing zone 2.0
--  13/06/23 Nareesh Komuravelly        Added database_replication_usage_history & commented role creation
--  14/07/23 Nareesh Komuravelly        Added 2 private listings objects for storage & compute
--  09/08/23 Nareesh Komuravelly       Added task error logs
--------------------------------------------------------------------

--Run the script using snowsql from the Chargeback directory:
--snowsql -c <env> -r sysadmin -D l_account_name=<account_name> -f chargebacks_account_satellite_deploy.sql -o output_file=chargebacks_account_satellite_deploy.out

------------------------------------------------------------------------------
-- snowsql session
--
!set variable_substitution=true
!set exit_on_error=True
!set results=true
!set echo=true

--------------------------------------------------------------------
-- environment definition
--
!print set environment definition

!define l_db_suffix=_db
!define l_wh_suffix=_wh
!define l_ar_suffix=_ar
!define l_fr_suffix=_fr

------------------------------------------------------------------------------
-- 000_admin
--
!print 000_admin

!print set context

!source accounts_config/&{l_account_name}_context.sql

!print show existing
show roles      like '&{l_entity_name}%';
show databases  like '&{l_target_db}%';
show schemas  like '&{l_target_schema}%';
show warehouses like '&{l_target_wh}%';

/*
---------------------CREATE SECURITYADMIN ROLE START-----------------------------
--!print CREATE SECURITYADMIN ROLE
--use role securityadmin;

-- keep create statement minimal
--create role if not exists &{l_entity_name}_securityadmin&{l_fr_suffix};

-- set properties with an alter statement to streamline maintenance
--alter role &{l_entity_name}_securityadmin&{l_fr_suffix} set    comment = 'Local securityadmin role.';

-- grant role to securityadmin
--grant role &{l_entity_name}_securityadmin&{l_fr_suffix}    to role securityadmin;
---------------------CREATE SECURITYADMIN ROLE END-----------------------------

---------------------CREATE SYSADMIM ROLE START-----------------------------
--!print CREATE SYSADMIM ROLE
--use role securityadmin;

-- keep create statement minimal
--create role if not exists &{l_entity_name}_sysadmin&{l_fr_suffix};

-- transfer ownership to local role
--grant ownership    on role &{l_entity_name}_sysadmin&{l_fr_suffix}    to role &{l_entity_name}_securityadmin&{l_fr_suffix};

--
-- finish setup with local role to ensure ownership was transferred successfully
--
--use role &{l_entity_name}_securityadmin&{l_fr_suffix};

-- set properties with an alter statement to streamline maintenance
--alter role &{l_entity_name}_sysadmin&{l_fr_suffix} set     comment = 'Local sysadmin role.';

-- grant role to sysadmin
--grant role &{l_entity_name}_sysadmin&{l_fr_suffix}     to role sysadmin;

---------------------CREATE SYSADMIM ROLE END-----------------------------

---------------------CREATE SYSADMIM_OPS ROLE START-----------------------
!print CREATE OPERATINAL ROLE

use role securityadmin;

create role if not exists &{l_entity_name}_OPS&{l_fr_suffix};

grant role &{l_entity_name}_OPS&{l_fr_suffix}     to role &{l_entity_name}_sysadmin&{l_fr_suffix};

---------------------CREATE SYSADMIM_OPS ROLE END-----------------------


---------------------GRANT TO NEW ROLES START-----------------------------

use role accountadmin;
-- give grants to new user to create the necessary objects
grant create database on account to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant create warehouse on account to role &{l_entity_name}_sysadmin&{l_fr_suffix};
grant imported privileges on database snowflake to role &{l_entity_name}_sysadmin&{l_fr_suffix};
GRANT EXECUTE TASK ON account TO role &{l_entity_name}_sysadmin&{l_fr_suffix};

---------------------GRANT TO NEW ROLES END----------------------------
*/

use role accountadmin;
GRANT EXECUTE MANAGED TASK ON account TO role &{l_entity_name}_sysadmin&{l_fr_suffix};
ALTER ACCOUNT SET ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR=true;
---------------------CREATE DATABASE AND SCHEMA START-----------------------------

!print CREATE DATABASE AND SCHEMA

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
-- Create new database
CREATE DATABASE IF NOT EXISTS &{l_target_db};
use database &{l_target_db};

-- Create new schema
CREATE SCHEMA IF NOT EXISTS &{l_target_schema};

CREATE SCHEMA IF NOT EXISTS &{l_sec_schema};
---------------------CREATE DATABASE AND SCHEMA END-----------------------------

---------------------CREATE WAREHOUSE START-----------------------------

!print CREATE WAREHOUSE

 -- Create new warehouse
 CREATE WAREHOUSE IF NOT EXISTS &{l_target_wh} WITH
     warehouse_size        = 'xsmall'
     auto_suspend          = 60
     auto_resume           = true
     comment               = 'Local warehouse for Cost monitoring workloads.'
 ;
 
---------------------CREATE WAREHOUSE END-----------------------------

use warehouse &{l_target_wh};
use database &{l_target_db};

DROP SCHEMA IF EXISTS PUBLIC;
use schema &{l_target_schema};

---------------------RUN DDL START-----------------------------
-- 5) create tables in schema

!print create tables in schema

!source 100_common/automatic_clustering_history_tbl.sql
!source 100_common/data_transfer_history_tbl.sql
!source 100_common/database_storage_usage_history_tbl.sql
!source 100_common/materialized_view_refresh_history_tbl.sql
!source 100_common/metering_daily_history_tbl.sql
!source 100_common/pipe_usage_history_tbl.sql
!source 100_common/replication_usage_history_tbl.sql
!source 100_common/database_replication_usage_history_tbl.sql
!source 100_common/search_optimization_history_tbl.sql
!source 100_common/stage_storage_usage_history_tbl.sql
!source 100_common/warehouse_metering_history_tbl.sql
!source 100_common/be_resource_mapping_lkp_tbl.sql
!source 100_common/serverless_task_history_tbl.sql
!source 100_common/query_acceleration_history_tbl.sql
!source 100_common/replication_group_usage_history_tbl.sql
--new added
!source 100_common/rau_warehouse_metering_stg_tbl.sql
!source 100_common/rau_warehouse_metering_hst_tbl.sql
!source 100_common/listing_auto_fulfillment_refresh_daily_tbl.sql
!source 100_common/listing_auto_fulfillment_database_storage_daily_tbl.sql

!source 100_common/v_unmapped_resources_tbl.sql
!source 100_common/v_unmapped_resources.sql

!source 100_common/task_error_logs_tbl.sql

-- 6) create tasks for SNOWFLAKE->STG ingestion

!print create tasks for SNOWFLAKE->STG ingestion

!source 100_common/initial_task.sql
!source 100_common/automatic_clustering_history_stg_task.sql
!source 100_common/data_transfer_history_stg_task.sql
!source 100_common/database_storage_usage_history_stg_task.sql
!source 100_common/materialized_view_refresh_history_stg_task.sql
!source 100_common/metering_daily_history_stg_task.sql
!source 100_common/pipe_usage_history_stg_task.sql
!source 100_common/replication_usage_history_stg_task.sql
!source 100_common/database_replication_usage_history_stg_task.sql
!source 100_common/search_optimization_history_stg_task.sql
!source 100_common/stage_storage_usage_history_stg_task.sql
!source 100_common/warehouse_metering_history_stg_task.sql
!source 100_common/serverless_task_history_stg_task.sql
!source 100_common/query_acceleration_history_stg_task.sql
!source 100_common/replication_group_usage_history_stg_task.sql
!source 100_common/rau_warehouse_metering_stg_task.sql
!source 100_common/listing_auto_fulfillment_refresh_daily_stg_task.sql
!source 100_common/listing_auto_fulfillment_database_storage_daily_stg_task.sql

-- 7) create tasks for delta date

!print create tasks dw_delta_date_task

-- 8) create tasks for stg->history ingestion

!print create tasks for stg->history ingestion

!source 100_common/automatic_clustering_history_hst_task.sql
!source 100_common/data_transfer_history_hst_task.sql
!source 100_common/database_storage_usage_history_hst_task.sql
!source 100_common/materialized_view_refresh_history_hst_task.sql
!source 100_common/metering_daily_history_hst_task.sql
!source 100_common/pipe_usage_history_hst_task.sql
!source 100_common/replication_usage_history_hst_task.sql
!source 100_common/database_replication_usage_history_hst_task.sql
!source 100_common/search_optimization_history_hst_task.sql
!source 100_common/stage_storage_usage_history_hst_task.sql
!source 100_common/warehouse_metering_history_hst_task.sql
!source 100_common/serverless_task_history_hst_task.sql
!source 100_common/query_acceleration_history_hst_task.sql
!source 100_common/replication_group_usage_history_hst_task.sql
!source 100_common/rau_warehouse_metering_hst_task.sql
!source 100_common/listing_auto_fulfillment_refresh_daily_hst_task.sql
!source 100_common/listing_auto_fulfillment_database_storage_daily_hst_task.sql

!source 100_common/v_unmapped_resources_task.sql
!source 100_common/task_error_logs_task.sql

---------------------GRANT PRIVILGES ON OPERATION OBJECTS START-----------------------------

USE ROLE SECURITYADMIN;
GRANT MONITOR,USAGE ON database &{l_target_db} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};

GRANT MONITOR,USAGE ON SCHEMA &{l_target_db}.&{l_target_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};
GRANT MONITOR,USAGE ON SCHEMA &{l_target_db}.&{l_sec_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};

GRANT USAGE, OPERATE on WAREHOUSE &{l_target_wh} to role &{l_entity_name}_OPS&{l_fr_suffix};

GRANT SELECT on ALL tables in SCHEMA &{l_target_db}.&{l_target_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};
GRANT SELECT on ALL tables in SCHEMA &{l_target_db}.&{l_sec_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};

GRANT SELECT on FUTURE tables in SCHEMA &{l_target_db}.&{l_target_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};
GRANT SELECT on FUTURE tables in SCHEMA &{l_target_db}.&{l_sec_schema} to ROLE &{l_entity_name}_OPS&{l_fr_suffix};

GRANT SELECT on ALL VIEWS IN SCHEMA &{l_target_db}.&{l_target_schema} to role &{l_entity_name}_OPS&{l_fr_suffix};
GRANT SELECT on ALL VIEWS IN SCHEMA &{l_target_db}.&{l_sec_schema} to role &{l_entity_name}_OPS&{l_fr_suffix};
GRANT SELECT on FUTURE VIEWS IN SCHEMA &{l_target_db}.&{l_target_schema} to role &{l_entity_name}_OPS&{l_fr_suffix};
GRANT SELECT on FUTURE VIEWS IN SCHEMA &{l_target_db}.&{l_sec_schema} to role &{l_entity_name}_OPS&{l_fr_suffix};

GRANT MONITOR ON ALL TASKS IN DATABASE &{l_target_db} TO ROLE &{l_entity_name}_OPS&{l_fr_suffix};

--read and write permission to LKP table
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE on &{l_target_db}.&{l_target_schema}.BE_RESOURCE_MAPPING_LKP
 to ROLE &{l_entity_name}_OPS&{l_fr_suffix};


---------------------GRANT PRIVILGES ON OPERATION OBJECTS END-----------------------------

-- 9) create share

!print create share

use role ACCOUNTADMIN;

!source 201_satellite/create_share.sql

-- 10) security monitoring
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_target_schema};
use warehouse &{l_target_wh};

!source 202_satellite_security/security_monitoring_deploy_overarching.sql

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
-- 11) resume tasks
!source 100_common/task_resume.sql

---------------------RUN DDL END-----------------------------


!print SUCCESS!
!quit

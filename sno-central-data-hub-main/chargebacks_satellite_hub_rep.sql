--------------------------------------------------------------------
--  Purpose: Satellite replication
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  08/12/21 Alessandro Dallatomasina   First release
--  24/02/22 Alessandro Dallatomasina   External configuration file added
--  08/09/22 Alessandro Dallatomasina   Landing zone 2.0
--------------------------------------------------------------------

--Run the script using snowsql from the Chargeback directory:
--snowsql -c <env> -r sysadmin -D l_account_name=<account_name> -f chargebacks_satellite_hub_rep.sql -o output_file=chargebacks_satellite_hub_replication.out

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


---------------------RUN DDL START-----------------------------

use role accountadmin;

use database &{l_target_db};

use schema &{l_target_schema};

--specify a warehouse
use warehouse &{l_target_wh};

----------- CREATE DATA REPLICATION DATABASE ------------------------
create database IF NOT EXISTS SAT_&{l_satellite_account}
as replica of &{l_hub_org_name}.&{l_satellite_account}.&{l_target_db}_data_rep;


GRANT OWNERSHIP ON DATABASE SAT_&{l_satellite_account} to role &{l_entity_name}_sysadmin&{l_fr_suffix};

GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE SAT_&{l_satellite_account} to role &{l_entity_name}_sysadmin&{l_fr_suffix} COPY CURRENT GRANTS;

GRANT OWNERSHIP ON ALL TABLES IN DATABASE SAT_&{l_satellite_account} to role &{l_entity_name}_sysadmin&{l_fr_suffix} COPY CURRENT GRANTS; 

use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use warehouse &{l_target_wh};
use database &{l_target_db};
use schema &{l_target_schema};
----------- CREATE TASK FOR REPLICATION DATABASE ------------------------

create or replace task refresh_&{l_satellite_account}_task
  warehouse = &{l_target_wh}
  schedule = '5 minute'  --This should be small for testing
as
  alter database SAT_&{l_satellite_account} refresh;
  
alter task refresh_&{l_satellite_account}_task resume;

execute task refresh_&{l_satellite_account}_task;
  
call system$wait(5, 'MINUTES');

CALL &{l_target_db}.&{l_target_schema}.all_history_view_proc();
  
  
---------------------RUN DDL END-----------------------------

!print SUCCESS!
!quit

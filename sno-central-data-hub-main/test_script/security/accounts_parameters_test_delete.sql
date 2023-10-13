
-------------------------------------
--Testing - Start
-------------------------------------
use role sno_central_monitoring_sysadmin_fr;
use schema sno_central_monitoring_raw_db.security;
use warehouse sno_central_monitoring_wh;

-------------------------------------
--account_parameters
-------------------------------------
-- match record counts & check if data is loaded
show parameters in account; --126 rec
select * from security.account_parameters_stg; --dw_load_ts should be current day, it has 126 rec

select * from security.account_parameters_history where effective_to is null; --126 records

select * from security.account_parameters_history where effective_from >= current_date();
--data comparison 
-- compare stg with history tables
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_stg
minus
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_history where effective_to is null;
--compare history with stg table
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_history where effective_to is null
minus
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_stg ;
--compare data between soruce & stg
show parameters in account;
SELECT "key","value","default","level" from table(RESULT_SCAN(LAST_QUERY_ID()))
MINUS
SELECT PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_stg
;

show parameters in account;
SELECT PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL from security.account_parameters_stg
MINUS
SELECT "key","value","default","level" from table(RESULT_SCAN(LAST_QUERY_ID()))
;
--data checks in target
select * from security.account_parameters_history; -- dw_event_shk should be always populated, org/acct/region should be always populated & be same

--test updates with data manipulation
select * from security.account_parameters_history where effective_to is not null;
select * from security.account_parameters_stg;

update security.account_parameters_history set parameter_value = true where parameter_name='UI_NOTIFICATIONS' ;
select * from security.account_parameters_history where parameter_name='UI_NOTIFICATIONS' ;

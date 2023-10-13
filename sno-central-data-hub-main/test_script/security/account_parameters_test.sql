-------------------------------------
-- Account Parameters (SHOW)
-- scd type 2
-------------------------------------
--truncate table security.account_parameters_history;
select * from security.account_parameters_stg; --126
select * from security.account_parameters_history;--126

--check for changes to ocnfirm type 2 is working
select effective_from, count(*) from security.account_parameters_history group by 1 order by 1 desc;
--check recent changes
select * from security.account_parameters_history where effective_from >= '2023-06-28 12:15:26.937 +0100';
--check for records that have been logically ended due to changes
select * from security.account_parameters_history where effective_to is not null;

--check for changes & ensure effective_to of old record is less than effective_from of new record
select * from security.account_parameters_history where parameter_name='ABORT_DETACHED_QUERY';
--****issue with effective_to < effective_from of new record*** -> Issue has been fixed now

--stg to history comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_STG
MINUS 
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_HISTORY
where (ACCOUNT_NAME, PARAMETER_NAME, EFFECTIVE_FROM) in (SELECT ACCOUNT_NAME, PARAMETER_NAME, MAX(EFFECTIVE_FROM) from SECURITY.ACCOUNT_PARAMETERS_HISTORY group by 1,2);

--history to stg comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_HISTORY
where (ACCOUNT_NAME, PARAMETER_NAME, EFFECTIVE_FROM) in (SELECT ACCOUNT_NAME, PARAMETER_NAME, MAX(EFFECTIVE_FROM) from SECURITY.ACCOUNT_PARAMETERS_HISTORY group by 1,2)
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_STG
;

--source to history comparison
show parameters in account;
SELECT "key","value","default","level" from table(RESULT_SCAN(LAST_QUERY_ID()))
MINUS
SELECT PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_HISTORY
where (ACCOUNT_NAME, PARAMETER_NAME, EFFECTIVE_FROM) in (SELECT ACCOUNT_NAME, PARAMETER_NAME, MAX(EFFECTIVE_FROM) from SECURITY.ACCOUNT_PARAMETERS_HISTORY group by 1,2);

--history to source comparison
show parameters in account;
SELECT PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.ACCOUNT_PARAMETERS_HISTORY
where (ACCOUNT_NAME, PARAMETER_NAME, EFFECTIVE_FROM) in (SELECT ACCOUNT_NAME, PARAMETER_NAME, MAX(EFFECTIVE_FROM) from SECURITY.ACCOUNT_PARAMETERS_HISTORY group by 1,2)
MINUS
SELECT "key","value","default","level" from table(RESULT_SCAN(LAST_QUERY_ID()));

--update 1 sample record to test SCD type 2
update SECURITY.ACCOUNT_PARAMETERS_HISTORY set parameter_value=true where parameter_name='ABORT_DETACHED_QUERY';
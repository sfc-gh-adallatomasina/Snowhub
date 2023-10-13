-------------------------------------
-- USER Parameters (SHOW)
-- scd type 2
-------------------------------------
use role accountadmin;
show users; --16 users
select * from security.USER_PARAMETERS_STG;--1410 recs
select * from security.USER_PARAMETERS_HISTORY; --1425 recs

select distinct user_name from security.USER_PARAMETERS_HISTORY; --15

--check which user is missing as counts dont match
show users;
select "login_name" from TABLE(RESULT_SCAN(last_query_id()))
minus
select distinct user_name from security.USER_PARAMETERS_HISTORY; 

select effective_from, count(*) from security.USER_PARAMETERS_HISTORY group by 1 order by 1; 
select * from security.USER_PARAMETERS_HISTORY where effective_to is not null; --type 2 is working

--check effective_from & effective_to for similar record to ensure they are getting populated in ocrrect order
select * from  security.USER_PARAMETERS_HISTORY where user_name='KARUPPASAMY.DEVADOSS@LSEG.COM' and parameter_name='ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR';

--stage to history comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.USER_PARAMETERS_STG
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.USER_PARAMETERS_HISTORY;

--history to stage comparison (comparing latest records as stg has all latest records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.USER_PARAMETERS_HISTORY where effective_to is null
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, USER_NAME, PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DEFAULT_VALUE, PARAMETER_LEVEL
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.USER_PARAMETERS_STG;

--not straight forward to compare to source as we loop & get data loaded into stg by user
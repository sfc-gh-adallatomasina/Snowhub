-------------------------------------
-- DESC Network Policy
-- scd type 2 (source is looped through each policy & not a simple select)
-- 
-------------------------------------

select * from security.network_policy_details_stg;--292
select * from security.network_policy_details_history;--294

--check if there were any changes to data & type 2 worked
select effective_from, count(*) from security.network_policy_details_history group by 1 order by 1 desc;
select * from security.network_policy_details_history where effective_to is not null;
--see changes
select * from security.network_policy_details_history where effective_from = '2023-07-04 04:30:31.057 +0100';
select * from security.network_policy_details_history where policy_value like '10.138%';

--stg to history comparison (source has all active latest records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.NETWORK_POLICY_DETAILS_STG
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.NETWORK_POLICY_DETAILS_HISTORY where effective_to is null;

--history to stage comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.NETWORK_POLICY_DETAILS_HISTORY where effective_to is null
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, POLICY_NAME, POLICY_TYPE, POLICY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.NETWORK_POLICY_DETAILS_STG;

--check no. of policis match with source
show network policies;--5
select distinct policy_name from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.NETWORK_POLICY_DETAILS_HISTORY where effective_to is null;

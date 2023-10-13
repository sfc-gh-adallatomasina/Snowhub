-------------------------------------
-- NETWORK POLICIES (SHOW)
-- scd type 2
-------------------------------------

use role accountadmin;
show network policies in account; --5

select * from security.network_policy_stg;--5
select * from security.network_policy_history; --5

select effective_from, count(*) from security.network_policy_history group by 1 order by 1;  -- there is no history

--updating a record manuallt & rerun to check type 2
update security.network_policy_history set entries_in_blocked_ip_list=-1 where policy_name='LSEG_SNOWFLAKE_LZ' and effective_to is null;

--checking type 2 updates
select effective_from, count(*) from security.network_policy_history group by 1 order by 1;  -- now I can see changes
--check changes data order
select * from security.network_policy_history where policy_name='LSEG_SNOWFLAKE_LZ';

--compare stg to history table
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_stg
minus
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_history where effective_to is null;

--compare  history to stg (stg has all records so comparing all latest records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_history where effective_to is null
minus
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_stg ;


--compare source to history table
show network policies in account;
select "created_on", "name", "entries_in_allowed_ip_list", "entries_in_blocked_ip_list"
from TABLE(RESULT_SCAN(LAST_QUERY_ID()))
minus
select CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_history where effective_to is null;

--compare history to source table
show network policies in account;
select CREATED_ON, POLICY_NAME, ENTRIES_IN_ALLOWED_IP_LIST, ENTRIES_IN_BLOCKED_IP_LIST
from security.network_policy_history where effective_to is null
minus
select "created_on", "name", "entries_in_allowed_ip_list", "entries_in_blocked_ip_list"
from TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;
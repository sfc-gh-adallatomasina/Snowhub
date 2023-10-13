
-------------------------------------
-- Managed Accounts (SHOW)
-- scd type 2
-- changes not getting captured in history (updates on comment was not checked)
-------------------------------------
use role accountadmin;
show managed accounts; --1

select * from security.managed_accounts_stg;--1
select * from security.managed_accounts_history;--1

select effective_from, count(*) from security.managed_accounts_history group by 1; --no changes

--update 1 record to test type 2 logic
update security.managed_accounts_history set comment='Test' where reader_acc_name='SNOLSELZDEV_READER';


--stg to target comparison (source has all active records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_stg
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_history where effective_to is null;

--target to source comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_history where effective_to is null
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_stg;

--source to target comparison
show managed accounts; 
select "name", "cloud", "region", "locator", "created_on", "url", "is_reader", "comment", "region_group" 
    FROM TABLE ( RESULT_SCAN ( last_query_id())) 
MINUS
select READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_history where effective_to is null;

--target to source comparison
show managed accounts; 
select READER_ACC_NAME, CLOUD, MANAGED_REGION, LOCATOR, CREATED_ON, URL, IS_READER, COMMENT, REGION_GROUP
from security.managed_accounts_history where effective_to is null
MINUS
select "name", "cloud", "region", "locator", "created_on", "url", "is_reader", "comment", "region_group" 
    FROM TABLE ( RESULT_SCAN ( last_query_id())) ;
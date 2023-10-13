-------------------------------------
-- Shares (SHOW)
-- scd type 2
-- issues found - target not getting loaded at all
-------------------------------------
use role accountadmin;
show shares; --5

select * from security.shares_stg; --5
select * from security.shares_history;

select effective_from, count(*) from security.shares_history group by 1; 

--no history so update a record & rerun
update security.shares_history set comment='Test' where effective_to is null and database_name='SNOWFLAKE_COST_MONITORING__PL';

select * from security.shares_history where database_name='SNOWFLAKE_COST_MONITORING__PL';

--stg to history comparison (shows all records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_stg
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_history where effective_to is null;

--history to stage comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_history where effective_to is null
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_stg;

--source to history comparison
show shares;
select "created_on", "kind", "name", "database_name", "to", "owner", "comment", "listing_global_name" 
    from table(RESULT_SCAN(LAST_QUERY_ID()))
MINUS
select CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_history where effective_to is null;

--history to soruce comparison
show shares;
select CREATED, SHARE_TYPE, SHARE_NAME, DATABASE_NAME, SHARE_TO, OWNER, COMMENT, LISTING_GLOBAL_NAME
from security.shares_history where effective_to is null
MINUS
select "created_on", "kind", "name", "database_name", "to", "owner", "comment", "listing_global_name" 
    from table(RESULT_SCAN(LAST_QUERY_ID()));
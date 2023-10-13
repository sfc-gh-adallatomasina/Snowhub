
-------------------------------------
--Testing - Start
-------------------------------------
use role sno_central_monitoring_sysadmin_fr;
use schema sno_central_monitoring_raw_db.security;
use warehouse sno_central_monitoring_wh;

-------------------------------------
--databases --delta with changes inserted as new records
--*new 2 columns added in soruce view i.e RESOURCE_GROUP & TYPE*
-------------------------------------

SELECT * FROM SECURITY.databases_STG; --check if there data for current day
select * from security.databases_hISTORY; -- check if data is populated & all columns are populated, 99 rec in tgt
select * from security.databases_hISTORY where dw_load_ts >= current_date(); --check if there is data for today

select * from snowflake.account_usage.databases;--actual source of data (contains 95 records)
select * from security.databases_hISTORY where (database_id,last_altered) in (select database_id, max(last_altered) from security.databases_hISTORY  group by 1); --match record counts with above, but select single version from target 

select DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, IS_TRANSIENT, COMMENT, CREATED, LAST_ALTERED, DELETED, RETENTION_TIME from snowflake.account_usage.databases
minus
select DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, IS_TRANSIENT, COMMENT, CREATED, LAST_ALTERED, DELETED, RETENTION_TIME 
  from security.databases_hISTORY where (database_id,last_altered) in (select database_id, max(last_altered) from security.databases_hISTORY  group by 1);

select DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, IS_TRANSIENT, COMMENT, CREATED, LAST_ALTERED, DELETED, RETENTION_TIME 
  from security.databases_hISTORY where (database_id,last_altered) in (select database_id, max(last_altered) from security.databases_hISTORY  group by 1)
minus
select DATABASE_ID, DATABASE_NAME, DATABASE_OWNER, IS_TRANSIENT, COMMENT, CREATED, LAST_ALTERED, DELETED, RETENTION_TIME from snowflake.account_usage.databases;

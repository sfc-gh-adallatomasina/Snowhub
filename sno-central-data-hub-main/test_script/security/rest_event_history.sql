-------------------------------------
--reset_event_history --incremental load
-------------------------------------
use schema sno_central_monitoring_raw_db.security;
select * from rest_event_history_stg; --69
select * from rest_event_history_history; --2.4K
-- all key fields seems ot be populated above

select dw_load_ts, count(*) from rest_event_history_history group by 1 order by 1 desc; --data getting loaded daily & today count matches stage
select MAX(EVENT_TIMESTAMP) from security.rest_event_history_history; -- data present as of today morning

--stg to history comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_STG
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_HISTORY
;

--history to stage comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_HISTORY
WHERE DW_LOAD_TS >= current_date()
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_STG
;

--source to history comparison
select
'LSEG'                 as ORGANIZATION_NAME
,'SNOLSELZDEV'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.EVENT_TIMESTAMP
,s.EVENT_ID
,s.EVENT_TYPE
,s.ENDPOINT
,s.METHOD
,s.STATUS
,s.ERROR_CODE
,s.DETAILS
,s.CLIENT_IP
,s.ACTOR_NAME
,s.ACTOR_DOMAIN
,s.RESOURCE_NAME
,s.RESOURCE_DOMAIN
,'REST EVENT HISTORY'
from table(snowflake.information_schema.rest_event_history(  rest_service_type => 'scim',
     time_range_start => dateadd('hours',-72,current_timestamp()),
     time_range_end => current_timestamp(),
     10000)) s 
where s.EVENT_TIMESTAMP <= (select  max( event_timestamp ) from rest_event_history_history)
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_HISTORY
;


--history to source comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, EVENT_TIMESTAMP, EVENT_ID, EVENT_TYPE, ENDPOINT, METHOD, STATUS, ERROR_CODE, DETAILS, CLIENT_IP, ACTOR_NAME, ACTOR_DOMAIN, RESOURCE_NAME, RESOURCE_DOMAIN, DW_FILE_NAME
FROM SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.REST_EVENT_HISTORY_HISTORY
WHERE DW_LOAD_TS >= current_date()
MINUS
select
'LSEG'                 as ORGANIZATION_NAME
,'SNOLSELZDEV'                as ACCOUNT_NAME
,current_region()                   as REGION_NAME
,s.EVENT_TIMESTAMP
,s.EVENT_ID
,s.EVENT_TYPE
,s.ENDPOINT
,s.METHOD
,s.STATUS
,s.ERROR_CODE
,s.DETAILS
,s.CLIENT_IP
,s.ACTOR_NAME
,s.ACTOR_DOMAIN
,s.RESOURCE_NAME
,s.RESOURCE_DOMAIN
,'REST EVENT HISTORY'
from table(snowflake.information_schema.rest_event_history(  rest_service_type => 'scim',
     time_range_start => dateadd('hours',-72,current_timestamp()),
     time_range_end => current_timestamp(),
     10000)) s 
where s.EVENT_TIMESTAMP <= (select  max( event_timestamp ) from rest_event_history_history);
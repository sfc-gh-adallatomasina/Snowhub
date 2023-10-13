-------------------------------------
-- DESC Integrations
-- scd type 2 (source is looped through each integration & not a simple select)
-- Issue(multiple records are getting created for 'SAML2_SNOWFLAKE_METADATA' -> SNOLSELZDEV	AWS_EU_WEST_1	AZUREADINTEGRATION)
-------------------------------------

select * from security.integration_details_stg;--46
select * from security.integration_details_history;--58

--check change history
select effective_from, count(*) from  security.integration_details_history group by 1 order by 1;

--check changes
select * from security.integration_details_history where effective_to is not null;
select * from security.integration_details_history where effective_from >= current_date();

--multiple records are getting created for 'SAML2_SNOWFLAKE_METADATA' -> SNOLSELZDEV	AWS_EU_WEST_1	AZUREADINTEGRATION
show integrations;
desc integration AZUREADINTEGRATION;

select * from security.integration_details_history  
  where INTEGRATION_NAME='AZUREADINTEGRATION' and property_name='SAML2_SNOWFLAKE_METADATA'
  order by effective_from;
select * from security.integration_details_stg 
  where INTEGRATION_NAME='AZUREADINTEGRATION' and property_name='SAML2_SNOWFLAKE_METADATA'
;

--stg to history comparison (soure has all records)
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, PROPERTY_NAME, PROPERTY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATION_DETAILS_STG
MINUS
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, PROPERTY_NAME, PROPERTY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATION_DETAILS_HISTORY where effective_to is null;

--history to stage comparison
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, PROPERTY_NAME, PROPERTY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATION_DETAILS_HISTORY where effective_to is null
minus
select ORGANIZATION_NAME, ACCOUNT_NAME, REGION_NAME, INTEGRATION_NAME, PROPERTY_NAME, PROPERTY_VALUE
from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATION_DETAILS_STG;

--source to history not performed
show integrations; -- 5
select distinct INTEGRATION_NAME from SNO_CENTRAL_MONITORING_RAW_DB.SECURITY.INTEGRATION_DETAILS_HISTORY where effective_to is null; --5

;
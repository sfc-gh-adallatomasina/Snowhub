--listing_auto_fulfillment_database_storage_daily
select region_group, snowflake_region, usage_date, database_name, source_database_id, deleted::timestamp_ltz, average_database_bytes, average_failsafe_bytes, listings
FROM snowflake.data_sharing_usage.listing_auto_fulfillment_database_storage_daily
MINUS
select region_group, snowflake_region, usage_date, database_name, source_database_id, deleted::timestamp_ltz, average_database_bytes, average_failsafe_bytes, listings
FROM listing_auto_fulfillment_database_storage_daily_history where account_name='SNOLSELZPROD';

select region_group, snowflake_region, usage_date, database_name, source_database_id, deleted::timestamp_ltz, average_database_bytes, average_failsafe_bytes, listings
FROM listing_auto_fulfillment_database_storage_daily_history where account_name='SNOLSELZPROD'
MINUS
select region_group, snowflake_region, usage_date, database_name, source_database_id, deleted::timestamp_ltz, average_database_bytes, average_failsafe_bytes, listings
FROM snowflake.data_sharing_usage.listing_auto_fulfillment_database_storage_daily;

--listing_auto_fulfillment_refresh_daily
SELECT REGION_GROUP, SNOWFLAKE_REGION, USAGE_DATE, FULFILLMENT_GROUP_NAME, BYTES_TRANSFERRED, CREDITS_USED, DATABASES, LISTINGS
FROM snowflake.data_sharing_usage.listing_auto_fulfillment_refresh_daily
MINUS
SELECT REGION_GROUP, SNOWFLAKE_REGION, USAGE_DATE, FULFILLMENT_GROUP_NAME, BYTES_TRANSFERRED, CREDITS_USED, DATABASES, LISTINGS
FROM LISTING_AUTO_FULFILLMENT_REFRESH_DAILY_HISTORY where account_name='SNOLSELZPROD';

SELECT REGION_GROUP, SNOWFLAKE_REGION, USAGE_DATE, FULFILLMENT_GROUP_NAME, BYTES_TRANSFERRED, CREDITS_USED, DATABASES, LISTINGS
FROM LISTING_AUTO_FULFILLMENT_REFRESH_DAILY_HISTORY where account_name='SNOLSELZPROD'
MINUS
SELECT REGION_GROUP, SNOWFLAKE_REGION, USAGE_DATE, FULFILLMENT_GROUP_NAME, BYTES_TRANSFERRED, CREDITS_USED, DATABASES, LISTINGS
FROM snowflake.data_sharing_usage.listing_auto_fulfillment_refresh_daily;
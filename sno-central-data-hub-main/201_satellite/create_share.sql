--------------------------------------------------------------------
--  Purpose: create satelite share
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 13/09/2022 Alessandro Dallatomasina 	V0.1
-- 14/07/2023 Nareesh komuravelly       Added db rep usage history as a replacement for rep usage history, and
--                                      Added view for private listings 
-- 09/08/2023 Nareesh Komuravelly       Added task error logs
--------------------------------------------------------------------

--
-- comment
--

CREATE SHARE IF NOT EXISTS satellite_share COMMENT='SNO_CENTRAL_MONITORING_SHARE';

GRANT USAGE ON DATABASE &{l_target_db} TO SHARE satellite_share;

GRANT USAGE ON SCHEMA &{l_target_db}.&{l_target_schema} TO SHARE satellite_share;

GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.automatic_clustering_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.be_resource_mapping_lkp TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.database_storage_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.data_transfer_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.materialized_view_refresh_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.metering_daily_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.pipe_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.replication_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.search_optimization_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.stage_storage_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.warehouse_metering_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.serverless_task_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.query_acceleration_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.replication_group_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.rau_warehouse_metering_history TO SHARE satellite_share;

GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.database_replication_usage_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history TO SHARE satellite_share;
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history TO SHARE satellite_share;

--unmapped_resource  view
GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.unmapped_resources TO SHARE satellite_share;

GRANT SELECT ON VIEW &{l_target_db}.&{l_target_schema}.task_error_logs TO SHARE satellite_share;

ALTER SHARE satellite_share ADD accounts = &{l_hub_org_name}.&{l_hub_account};

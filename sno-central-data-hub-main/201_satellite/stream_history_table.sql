--------------------------------------------------------------------
--  Purpose: create streams
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------- --------------------------------------------------------------------
--  dd/mm/yy
--  13/06/23 Nareesh Komuravelly  Added database_replication_usage_history
--  14/05/23 Nareesh Komuravelly  Added 2 tables for private listings
--  09/08/23 Nareesh Komuravelly  Added task_error_logs to capture failures in sat accounts
-----------------------------------------------------------------------------------------------

--
-- comment
--

create stream if not exists automatic_clustering_history_stream on table &{l_target_db}.&{l_target_schema}.automatic_clustering_history append_only = true;
--create stream be_resource_mapping_lkp_stream on table &{l_target_db}.&{l_target_schema}.be_resource_mapping_lkp append_only = true;

create stream if not exists data_transfer_history_stream on table &{l_target_db}.&{l_target_schema}.data_transfer_history append_only = true;

create stream if not exists database_storage_usage_history_stream on table &{l_target_db}.&{l_target_schema}.database_storage_usage_history append_only = true;

create stream if not exists materialized_view_refresh_history_stream on table &{l_target_db}.&{l_target_schema}.materialized_view_refresh_history append_only = true;

create stream if not exists metering_daily_history_stream on table &{l_target_db}.&{l_target_schema}.metering_daily_history append_only = true;

create stream if not exists pipe_usage_history_stream on table &{l_target_db}.&{l_target_schema}.pipe_usage_history append_only = true;

--create stream if not exists replication_usage_history_stream on table &{l_target_db}.&{l_target_schema}.replication_usage_history append_only = true;
DROP STREAM IF EXISTS replication_usage_history_stream;

create stream if not exists database_replication_usage_history_stream on table &{l_target_db}.&{l_target_schema}.database_replication_usage_history append_only = true;

create stream if not exists search_optimization_history_stream on table &{l_target_db}.&{l_target_schema}.search_optimization_history append_only = true;

create stream if not exists stage_storage_usage_history_stream on table &{l_target_db}.&{l_target_schema}.stage_storage_usage_history append_only = true;

create stream if not exists warehouse_metering_history_stream on table &{l_target_db}.&{l_target_schema}.warehouse_metering_history append_only = true;

create stream if not exists query_acceleration_history_stream on table &{l_target_db}.&{l_target_schema}.query_acceleration_history append_only = true;

create stream if not exists replication_group_usage_history_stream on table &{l_target_db}.&{l_target_schema}.replication_group_usage_history append_only = true;

create stream if not exists serverless_task_history_stream on table &{l_target_db}.&{l_target_schema}.SERVERLESS_TASK_HISTORY append_only = true;

create stream if not exists rau_warehouse_metering_history_stream on table &{l_target_db}.&{l_target_schema}.rau_warehouse_metering_history append_only = true;

create stream if not exists listing_auto_fulfillment_refresh_daily_history_stream on table &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history append_only = true;

create stream if not exists listing_auto_fulfillment_database_storage_daily_history_stream on table &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history append_only = true;

create stream if not exists task_error_logs_stream on table &{l_target_db}.&{l_target_schema}.task_error_logs append_only = true;

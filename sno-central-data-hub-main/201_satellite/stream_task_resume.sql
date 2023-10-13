--------------------------------------------------------------------
--  Purpose: resume tasks
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------- --------------------------------------------------------------------
--  dd/mm/yy
--  13/06/23 Nareesh Komuravelly  Added database_replication_usage_history
--  14/07/23 Nareesh Komuravelly  Added new private listing views
--  09/08/23 Nareesh Komuravelly  Added task_error_logs to capture failures in sat accounts
-----------------------------------------------------------------------------------------------


ALTER TASK TASK_LOAD_warehouse_metering_history_ins RESUME;

ALTER TASK TASK_LOAD_stage_storage_usage_history_ins RESUME;

ALTER TASK TASK_LOAD_search_optimization_history_ins RESUME;

--ALTER TASK TASK_LOAD_replication_usage_history_ins RESUME;

ALTER TASK task_load_database_replication_usage_history_ins RESUME;

ALTER TASK TASK_LOAD_pipe_usage_history_ins RESUME;

ALTER TASK TASK_LOAD_metering_daily_history_ins RESUME;

ALTER TASK TASK_LOAD_materialized_view_refresh_history_ins RESUME;

ALTER TASK TASK_LOAD_database_storage_usage_history_ins RESUME;

ALTER TASK TASK_LOAD_data_transfer_history_ins RESUME;

ALTER TASK TASK_LOAD_be_resource_mapping_lkp_ins RESUME;

ALTER TASK TASK_LOAD_automatic_clustering_history_ins resume;

ALTER TASK task_load_serverless_task_history_ins RESUME;

ALTER TASK task_load_query_acceleration_history_ins RESUME;

ALTER TASK task_load_replication_group_usage_history_ins RESUME;

ALTER TASK task_load_rau_warehouse_metering_history_ins RESUME;

ALTER TASK task_load_listing_auto_fulfillment_refresh_daily_history_ins RESUME;

ALTER TASK task_load_listing_auto_fulfillment_database_storage_daily_history_ins RESUME;

ALTER TASK V_UNMAPPED_TASK RESUME;

ALTER TASK task_load_task_error_logs_ins RESUME;



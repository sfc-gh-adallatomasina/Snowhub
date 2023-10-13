--------------------------------------------------------------------
--  Purpose: identify unmapped resources
--
--  Revision History:
--  Date     Engineer                      Description
--  -------- ----------------------------- ---------------------------------------------
-- 09/03/2022 	Alessandro Dallatomasina 	  Decentralising be_resource_mapping_lkp table
-- 13/06/2023   Nareesh Komuravelly         Added database replication usage history
-- 14/07/2023   Nareesh Komuravelly         Added private listings objects
-----------------------------------------------------------------------------------------


CREATE OR REPLACE VIEW &{l_target_db}.&{l_target_schema}.v_unmapped_resources_all as
SELECT * FROM (with l_resource_mapping as
(select organization_name, resource_name, resource_type_cd from &{l_target_db}.&{l_target_schema}.be_resource_mapping_lkp_all)
  select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'        as account_name
        ,current_region()          as region_name
        ,wmh.warehouse_name        as resource_name
        ,'warehouse'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.warehouse_metering_history_all wmh
       LEFT JOIN l_resource_mapping brml ON wmh.warehouse_name=brml.resource_name AND brml.resource_type_cd='warehouse' 
   UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,qch.warehouse_name        as resource_name
        ,'warehouse'               as resource_type_cd
    from
       snowflake.account_usage.query_acceleration_history_all qch
       LEFT JOIN l_resource_mapping brml ON qch.warehouse_name=brml.resource_name AND brml.resource_type_cd='warehouse' 

    UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,rgh.replication_group_name        as resource_name
        ,'database'                as resource_type_cd
    from
       snowflake.account_usage.replication_group_usage_history_all rgh
       LEFT JOIN l_resource_mapping brml ON rgh.replication_group_name=brml.resource_name AND brml.resource_type_cd='database' 

    union
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,sth.database_name         as resource_name
        ,'warehouse'               as resource_type_cd
    from
       snowflake.account_usage.serverless_task_history_all sth
        LEFT JOIN l_resource_mapping brml ON sth.database_name=brml.resource_name AND brml.resource_type_cd='warehouse'  
   UNION 
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,dsuh.database_name        as resource_name
        ,'database'                as resource_type_cd
    from
      &{l_target_db}.&{l_target_schema}.database_storage_usage_history_all dsuh
      LEFT JOIN l_resource_mapping brml ON dsuh.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,'INTERNAL STAGE'          as resource_name
        ,'internal_stage'          as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.stage_storage_usage_history_all ssuh
       LEFT JOIN l_resource_mapping brml ON brml.resource_name='INTERNAL STAGE' AND brml.resource_type_cd='internal_stage' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ach.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.automatic_clustering_history_all ach
       LEFT JOIN l_resource_mapping brml ON ach.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,mvrh.database_name        as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.materialized_view_refresh_history_all mvrh
       LEFT JOIN l_resource_mapping brml ON mvrh.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ifnull(p.pipe_catalog, 'Unknown')        as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.pipe_usage_history_all puh
        LEFT JOIN snowflake.account_usage.pipes p on p.pipe_id = puh.pipe_id
        LEFT JOIN l_resource_mapping brml ON ifnull(p.pipe_catalog, 'Unknown')=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,soh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.search_optimization_history_all soh
       LEFT JOIN l_resource_mapping brml ON soh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ruh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.replication_usage_history_all ruh
        LEFT JOIN l_resource_mapping brml ON ruh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ruh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.database_replication_usage_history_all ruh
        LEFT JOIN l_resource_mapping brml ON ruh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )   as resource_name
        ,'cloudregion'                                                                      as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.data_transfer_history_all dth
       LEFT JOIN l_resource_mapping brml ON concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )=brml.resource_name AND brml.resource_type_cd='cloudregion'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ldsth.database_name       as resource_name
        ,'database'                                                                         as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history_all ldsth
       LEFT JOIN l_resource_mapping brml ON ldsth.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,lrdh.fulfillment_group_name       as resource_name
        ,'database'                                                                         as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history_all lrdh
       LEFT JOIN l_resource_mapping brml ON lrdh.fulfillment_group_name=brml.resource_name AND brml.resource_type_cd='database'	   
  ) WHERE ORGANIZATION_NAME IS NULL
  
  ;

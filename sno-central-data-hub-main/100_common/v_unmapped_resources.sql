--------------------------------------------------------------------
--  Purpose: identify unmapped resources
--
--  Revision History:
--  Date     Engineer                       Description
--  -------- ------------------------------ ---------------------------------------------------------------------
-- 09/03/2022 	Alessandro Dallatomasina 	Decentralising be_resource_mapping_lkp table
-- 15/03/2023 	sayali phadtare 	        updated for SNOWFLAKE.READER_ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- 17/03/2023   sayali phadtare             view changed to secure view
-- 13/06/2023   Nareesh Komuravelly         Added database replication usage history
-- 14/07/2023   Nareesh Komuravelly         Added private listings objects
------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE SECURE VIEW &{l_target_db}.&{l_target_schema}.v_unmapped_resources as
SELECT * FROM (with l_resource_mapping as
(select organization_name, resource_name, resource_type_cd from &{l_target_db}.&{l_target_schema}.BE_RESOURCE_MAPPING_LKP)
  select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,wmh.warehouse_name        as resource_name
        ,'warehouse'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.warehouse_metering_history wmh
       LEFT JOIN l_resource_mapping brml ON wmh.warehouse_name=brml.resource_name AND brml.resource_type_cd='warehouse' 

   UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,rwmh.warehouse_name       as resource_name
        ,'warehouse'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.RAU_WAREHOUSE_METERING_HISTORY rwmh
       LEFT JOIN l_resource_mapping brml ON rwmh.warehouse_name=brml.resource_name AND brml.resource_type_cd='warehouse' 

   UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,qch.warehouse_name        as resource_name
        ,'warehouse'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.query_acceleration_history qch
       LEFT JOIN l_resource_mapping brml ON qch.warehouse_name=brml.resource_name AND brml.resource_type_cd='warehouse' 

    UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,rgh.replication_group_name        as resource_name
        ,'database'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.replication_group_usage_history rgh
       LEFT JOIN l_resource_mapping brml ON rgh.replication_group_name=brml.resource_name AND brml.resource_type_cd='database' 

    union
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,sth.database_name         as resource_name
        ,'warehouse'               as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.serverless_task_history sth
        LEFT JOIN l_resource_mapping brml ON sth.database_name=brml.resource_name AND brml.resource_type_cd='warehouse'  
   UNION 
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,dsuh.database_name        as resource_name
        ,'database'                as resource_type_cd
    from
      &{l_target_db}.&{l_target_schema}.database_storage_usage_history dsuh
      LEFT JOIN l_resource_mapping brml ON dsuh.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
   select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,'INTERNAL STAGE'          as resource_name
        ,'internal_stage'          as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.stage_storage_usage_history ssuh
       LEFT JOIN l_resource_mapping brml ON brml.resource_name='INTERNAL STAGE' AND brml.resource_type_cd='internal_stage' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ach.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.automatic_clustering_history ach
       LEFT JOIN l_resource_mapping brml ON ach.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,mvrh.database_name        as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.materialized_view_refresh_history mvrh
       LEFT JOIN l_resource_mapping brml ON mvrh.database_name=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ifnull(puh.database_name, 'Unknown')        as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.pipe_usage_history puh
    LEFT JOIN l_resource_mapping brml ON ifnull(puh.database_name, 'Unknown')=brml.resource_name AND brml.resource_type_cd='database' 
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,soh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.search_optimization_history soh
       LEFT JOIN l_resource_mapping brml ON soh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ruh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.replication_usage_history ruh
        LEFT JOIN l_resource_mapping brml ON ruh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ruh.database_name         as resource_name
        ,'database'                as resource_type_cd
    from
        &{l_target_db}.&{l_target_schema}.database_replication_usage_history ruh
        LEFT JOIN l_resource_mapping brml ON ruh.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )   as resource_name
        ,'cloudregion'                                                                      as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.data_transfer_history dth
       LEFT JOIN l_resource_mapping brml ON concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )=brml.resource_name AND brml.resource_type_cd='cloudregion'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,ldsth.database_name       as resource_name
        ,'database'                as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_database_storage_daily_history ldsth
       LEFT JOIN l_resource_mapping brml ON ldsth.database_name=brml.resource_name AND brml.resource_type_cd='database'
  UNION
    select distinct
         brml.organization_name    as organization_name
        ,'&{l_account_name}'       as account_name
        ,current_region()          as region_name
        ,lrdh.fulfillment_group_name       as resource_name
        ,'warehouse'                       as resource_type_cd
    from
       &{l_target_db}.&{l_target_schema}.listing_auto_fulfillment_refresh_daily_history lrdh
       LEFT JOIN l_resource_mapping brml ON lrdh.fulfillment_group_name=brml.resource_name AND brml.resource_type_cd='warehouse'
	) 
WHERE ORGANIZATION_NAME IS NULL             
;

--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 30/11/21 	Alessandro Dallatomasina 	Removed the JSON and added APPID and Project code
-- 09/03/2022 	Alessandro Dallatomasina 	Decentralising cb_service_type_lkp table
--------------------------------------------------------------------

--
-- permanent table with retention days
--


create table if not exists &{l_target_db}.&{l_target_schema}.be_resource_mapping_lkp
(
     organization_name   varchar( 100 )
    ,account_name        varchar( 100 )
    ,region_name         varchar( 100 )
    ,resource_name       varchar( 100 )
    ,resource_type_cd    varchar( 100 )
    ,business_entity     varchar( 100 )
    ,environment         varchar( 100 )
    ,team                varchar( 100 )
    ,application_name    varchar( 100 )
    ,appid               varchar( 100 )
    ,project_code        varchar( 100 )
	,cost_centre		 varchar( 100 )
    ,priority_no         number
    ,dw_load_ts          timestamp_tz not null
)
data_retention_time_in_days = 1
;
/*
EXECUTE IMMEDIATE $$
BEGIN
  IF (NOT EXISTS(SELECT * 
                 FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_CATALOG = 'SNO_CENTRAL_MONITORING_RAW_DB'
                   AND TABLE_NAME = 'BE_RESOURCE_MAPPING_LKP' 
                   AND TABLE_SCHEMA = 'LANDING' 
                   AND COLUMN_NAME = 'BUSINESS_DIVISION')) THEN
    ALTER TABLE IF EXISTS BE_RESOURCE_MAPPING_LKP ADD COLUMN BUSINESS_DIVISION VARCHAR;
  END IF;
  IF (NOT EXISTS(SELECT * 
                 FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_CATALOG = 'SNO_CENTRAL_MONITORING_RAW_DB'
                   AND TABLE_NAME = 'BE_RESOURCE_MAPPING_LKP' 
                   AND TABLE_SCHEMA = 'LANDING' 
                   AND COLUMN_NAME = 'SUB_DIVISION')) THEN
    ALTER TABLE IF EXISTS BE_RESOURCE_MAPPING_LKP ADD COLUMN SUB_DIVISION VARCHAR;
  END IF;
END;
$$
;
*/
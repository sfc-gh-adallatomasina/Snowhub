--------------------------------------------------------------------
--  Purpose: Populate Replica table
--
--  Revision History:
--  Date     Engineer             Description
--  -------- ------------- --------------------------------------------------------------------
--  dd/mm/yy
--  13/06/23 Nareesh Komuravelly  Added new columns
-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK task_load_be_resource_mapping_lkp_ins
--WAREHOUSE = &{l_target_wh}
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 AFTER &{l_target_db}.&{l_target_schema}.TASK_INITIALIZE
--WHEN SYSTEM$STREAM_HAS_DATA('be_resource_mapping_lkp_stream')
AS
EXECUTE IMMEDIATE $$
BEGIN 
 ALTER SESSION SET TIMEZONE = 'Europe/London';
insert overwrite into SNO_CENTRAL_MONITORING_RAW_DB_data_rep.LANDING.be_resource_mapping_lkp
select organization_name   
    ,account_name        
    ,region_name         
    ,resource_name       
    ,resource_type_cd    
    ,business_entity     
    ,environment         
    ,team                
    ,application_name    
    ,appid               
    ,project_code        
	,cost_centre		 
    ,priority_no       
    ,dw_load_ts 
    --,BUSINESS_DIVISION   
    --,SUB_DIVISION        
 from &{l_target_db}.&{l_target_schema}.be_resource_mapping_lkp;
END;
$$;

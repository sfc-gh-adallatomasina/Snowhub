--------------------------------------------------------------------
--  Purpose: identify unmapped resources
--
--  Revision History:
--  Date     Engineer                     Description
--  -------- ---------------------------- ----------------------------------------------------------
-- 13/09/2022 	Alessandro Dallatomasina 	Create *_all views for first HUB deployment 
-- 20/03/2023   Sayali Phadtare           update view for unionall and v_unmapped_resources
-- 13/06/2023   Nareesh Komuravelly       Added database_replication_usage_history
-- 14/07/2023   Nareesh Komuravelly       Added private listings objects
-- 09/08/2023   Nareesh Komuravelly       Added task error logs
-----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_target_schema}.all_history_view_proc()
  returns VARCHAR -- return final create statement
  language javascript
  as
  $$

	//CREATE AN ARRAY
  //removing 'be_resource_mapping_lkp' temporarily until changes are deployed in all accounts, when new columns were not present in all tables
	var list_table_name = ['be_resource_mapping_lkp','automatic_clustering_history','materialized_view_refresh_history','metering_daily_history'
  ,'pipe_usage_history','replication_usage_history','database_replication_usage_history','search_optimization_history','stage_storage_usage_history'
  ,'warehouse_metering_history','data_transfer_history','database_storage_usage_history','serverless_task_history','query_acceleration_history'
  ,'replication_group_usage_history','rau_warehouse_metering_history','unmapped_resources','listing_auto_fulfillment_refresh_daily_history'
  ,'listing_auto_fulfillment_database_storage_daily_history','task_error_logs'];

  var len = list_table_name.length;

	// LOOP OVER EACH TABLE

	for(i=0;i<len;i++){
    var sql_statement="";
		// build query to get databases from information_schema
	var get_databases_stmt = "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE ('SAT_%') OR DATABASE_NAME='SNO_CENTRAL_MONITORING_RAW_DB'"

	var get_databases_stmt= snowflake.createStatement({sqlText:get_databases_stmt });

  //get result set containing all database names
   var database_list=get_databases_stmt.execute();

 // loop over DATABASE set to build statement

   while(database_list.next()){
     var database_name=database_list.getColumnValue(1);

 //table is present in database or not
     var st="SELECT TABLE_NAME from " + database_name +".INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=upper"+"(" + "'" +list_table_name[i]+"'"+  ")";

     var table_list= snowflake.execute({sqlText: st});
     
      if (table_list.getRowCount() > 0)
                       {
                                      sql_statement +="SELECT * FROM " + database_name + ".LANDING." + list_table_name[i] +" UNION ALL ";
                       }
    }//END WHILE
  
 
    
    if (sql_statement.length > 0)
        {
 //remove last union all	
                 sql_statement = sql_statement.slice(0, -11);

 // create the view; one for each table
                 sql_statement = "CREATE OR REPLACE VIEW &{l_target_db}.&{l_target_schema}."+ list_table_name[i] + "_ALL " + " AS " + sql_statement;
                 snowflake.execute({sqlText: sql_statement});
    }
    else{
    continue;
         }
 
}
 return "ALL views created successfully";

$$;

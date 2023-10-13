--------------------------------------------------------------------
--  Purpose: identify unmapped resources
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 13/09/2022 	Alessandro Dallatomasina 	Create *_all views for first HUB deployment 
--------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE &{l_target_db}.&{l_target_schema}.all_history_view_proc()
  returns VARCHAR -- return final create statement
  language javascript
  as
  $$

	//CREATE AN ARRAY

	var list_table_name = ['be_resource_mapping_lkp','automatic_clustering_history','materialized_view_refresh_history','metering_daily_history','pipe_usage_history',
	'replication_usage_history','search_optimization_history','stage_storage_usage_history','warehouse_metering_history','data_transfer_history','database_storage_usage_history',
	'serverless_task_history','query_acceleration_history','replication_group_usage_history'];

    var len = list_table_name.length;

	// LOOP OVER EACH TABLE

	for(i=0;i<len;i++){

		// build query to get databases from information_schema
		var get_databases_stmt = "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE ('SAT_%') OR DATABASE_NAME='SNO_CENTRAL_MONITORING_RAW_DB'"

		var get_databases_stmt = snowflake.createStatement({sqlText:get_databases_stmt });
		
		//get result set containing all table names
		var databases = get_databases_stmt.execute();

		// to control if UNION ALL should be added or not
		var row_count = get_databases_stmt.getRowCount();
		var rows_iterated = 0;

		// create the view; one for each table
		var create_statement = "CREATE OR REPLACE VIEW &{l_target_db}.&{l_target_schema}."+list_table_name[i]+"_ALL AS \n";

		// loop over DATABASE set to build statement

		while (databases.next())  {
			rows_iterated += 1;

			// we get values from the first (and only) column in the result set
			var database_name = databases.getColumnValue(1);
			
			// this will obviously fail if the column count doesnt match
			create_statement += "SELECT * FROM "+database_name+".LANDING."+list_table_name[i];
			
			// add union all to all but last row
			if (rows_iterated < row_count){
            create_statement += "\n UNION ALL \n"
			}

		}

    // create the view
	create_statement = snowflake.createStatement( {sqlText: create_statement} );
	create_statement.execute();

    // Reset for the next Table
    rows_iterated = 0;

    }
	
	// return the create statement as text
	// return create_statement.getSqlText(); --for testing

  $$
  ;

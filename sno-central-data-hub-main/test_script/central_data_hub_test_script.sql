use role SNO_CENTRAL_MONITORING_SYSADMIN_FR;
use warehouse SNO_CENTRAL_MONITORING_WH;
use database  SNO_CENTRAL_MONITORING_RAW_DB;
use schema LANDING;

select current_date();
--select CURRENT_ACCOUNT();

ALTER SESSION SET TIMEZONE = 'Europe/London';

----------------------------------------------Automatic Clustering History-------------------------------
set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME                     
  from snowflake.account_usage.AUTOMATIC_CLUSTERING_HISTORY
                    WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from AUTOMATIC_CLUSTERING_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from AUTOMATIC_CLUSTERING_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from
SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
 WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from AUTOMATIC_CLUSTERING_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.AUTOMATIC_CLUSTERING_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));





set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from AUTOMATIC_CLUSTERING_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.AUTOMATIC_CLUSTERING_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from AUTOMATIC_CLUSTERING_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select  START_TIME,END_TIME,CREDITS_USED,NUM_BYTES_RECLUSTERED,NUM_ROWS_RECLUSTERED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.AUTOMATIC_CLUSTERING_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
)
                    );

--Print Result----

--Expected Count for AUTOMATIC_CLUSTERING FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for AUTOMATIC_CLUSTERING (STAGE TABLE)
select $delta_count_stage;

---Record Count matches for AUTOMATIC_CLUSTERING (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for AUTOMATIC_CLUSTERING (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for AUTOMATIC_CLUSTERING_HISTORY
select $delta_count_usage;

--Actual count for AUTOMATIC_CLUSTERING_HISTORY 
select $delta_count_hist;

--Record Count Matches for AUTOMATIC_CLUSTERING_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for AUTOMATIC_CLUSTERING_HISTORY
select $Unmatched_data;





-----------------------------------------------DATABASE_STORAGE_USAGE_HISTORY------------------------------
set delta_count_usage_stg=(
SELECT COUNT(*) from(
select  convert_timezone('Europe/London', usage_date)::timestamp_ntz,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES
from snowflake.account_usage.database_storage_usage_history where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from database_storage_usage_history where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
));

set delta_count_stage=(select count(*) from database_storage_usage_history_stg where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from(
select  convert_timezone('Europe/London', usage_date)::timestamp_ntz,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES
from snowflake.account_usage.database_storage_usage_history where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from database_storage_usage_history where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select  USAGE_DATE,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES from
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.DATABASE_STORAGE_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));



set delta_count_usage=(select count(*) from
(select  convert_timezone('Europe/London', usage_date)::date,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY 
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from DATABASE_STORAGE_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date));


set delta_count_hist=(select count(*) from DATABASE_STORAGE_USAGE_HISTORY where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
(select  convert_timezone('Europe/London', usage_date)::date,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES
 from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from DATABASE_STORAGE_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
MINUS
select  USAGE_DATE,DATABASE_ID,DATABASE_NAME,AVERAGE_DATABASE_BYTES,AVERAGE_FAILSAFE_BYTES from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.DATABASE_STORAGE_USAGE_HISTORY_STG
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));


--Print Result----

--Expected Count for DATABASE_STORAGE_USAGE FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for DATABASE_STORAGE_USAGE (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for DATABASE_STORAGE_USAGE (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for DATABASE_STORAGE_USAGE (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for DATABASE_STORAGE_USAGE_HISTORY
select $delta_count_usage;

--Actual count for DATABASE_STORAGE_USAGE_HISTORY
select $delta_count_hist;

--Record Count Matches for DATABASE_STORAGE_USAGE_HISTORY
select $Unmatched_records;

--No of records failing data comparison for DATABASE_STORAGE_USAGE_HISTORY
select $Unmatched_data;




-------------------------------------------------------------data transfer history-------------------------------
set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE                      
  from snowflake.account_usage.DATA_TRANSFER_HISTORY
                   WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from DATA_TRANSFER_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from DATA_TRANSFER_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
 (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE from
SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
 WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from DATA_TRANSFER_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.DATA_TRANSFER_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));
  


set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE
 from SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from DATA_TRANSFER_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.DATA_TRANSFER_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE
 from SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from DATA_TRANSFER_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select START_TIME,END_TIME,SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION,BYTES_TRANSFERRED,TRANSFER_TYPE from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.DATA_TRANSFER_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
)
                    );

--Print Result----

--Expected Count for DATA_TRANSFER FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for DATA_TRANSFER (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for DATA_TRANSFER (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for DATA_TRANSFER (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for DATA_TRANSFER_HISTORY
select $delta_count_usage;

--Actual count for DATA_TRANSFER_HISTORY 
select $delta_count_hist;

--Record Count Matches for DATA_TRANSFER_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for DATA_TRANSFER_HISTORY 
select $Unmatched_data;



-----------------------------------Materialized view history--------------------------


set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
 (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME                      
  from snowflake.account_usage.MATERIALIZED_VIEW_REFRESH_HISTORY
                  WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from MATERIALIZED_VIEW_REFRESH_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from MATERIALIZED_VIEW_REFRESH_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
 (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from
SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from MATERIALIZED_VIEW_REFRESH_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.MATERIALIZED_VIEW_REFRESH_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));


  



set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from MATERIALIZED_VIEW_REFRESH_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.MATERIALIZED_VIEW_REFRESH_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from MATERIALIZED_VIEW_REFRESH_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select START_TIME,END_TIME,CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.MATERIALIZED_VIEW_REFRESH_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
)
                    );

--Print Result----

--Expected Count for MATERIALIZED_VIEW_REFRESH FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for MATERIALIZED_VIEW_REFRESH (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for MATERIALIZED_VIEW_REFRESH (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for MATERIALIZED_VIEW_REFRESH (STAGE TABLE)
select $Unmatched_data_stage;


--Expected Count for MATERIALIZED_VIEW_REFRESH_HISTORY
select $delta_count_usage;

--Actual count for MATERIALIZED_VIEW_REFRESH_HISTORY 
select $delta_count_hist;

--Record Count Matches for MATERIALIZED_VIEW_REFRESH_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for MATERIALIZED_VIEW_REFRESH_HISTORY 
select $Unmatched_data;


--------------------------------------------------------Metering--daily-----History-----------

set delta_count_usage_stg=(
SELECT COUNT(*)FROM
(select  SERVICE_TYPE, convert_timezone('Europe/London', usage_date)::timestamp_ntz,
CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED from
 snowflake.account_usage.METERING_DAILY_HISTORY
where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from METERING_DAILY_HISTORY where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
));

set delta_count_stage=(select count(*) from METERING_DAILY_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from(
select  SERVICE_TYPE, convert_timezone('Europe/London', usage_date)::timestamp_tz,
CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED
        FROM snowflake.account_usage.METERING_DAILY_HISTORY
where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from METERING_DAILY_HISTORY where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select SERVICE_TYPE,USAGE_DATE,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED from
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.METERING_DAILY_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));



set delta_count_usage=(select count(*) from
(select  SERVICE_TYPE, convert_timezone('Europe/London', usage_date)::timestamp_ntz,
CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED
 from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY 
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from METERING_DAILY_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
));


set delta_count_hist=(select count(*) from METERING_DAILY_HISTORY where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);


set Unmatched_data=(select count(*) from 
(select SERVICE_TYPE, convert_timezone('Europe/London', usage_date)::date,
CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED
 from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from METERING_DAILY_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
MINUS
select  SERVICE_TYPE,USAGE_DATE,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,CREDITS_USED,CREDITS_ADJUSTMENT_CLOUD_SERVICES,CREDITS_BILLED from
 SNO_CENTRAL_MONITORING_RAW_DB.LANDING.METERING_DAILY_HISTORY_STG
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));


--Print Result----

--Expected Count for METERING_DAILY FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for METERING_DAILY (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for METERING_DAILY (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for METERING_DAILY (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for METERING DAILY HISTORY 
select $delta_count_usage;

--Actual count for METERING DAILY HISTORY 
select $delta_count_hist;

--Record Count Matches for METERING DAILY HISTORY 
select $Unmatched_records;

--No of records failing data comparison for METERING DAILY HISTORY 
select $Unmatched_data;



-----------------------------------------------Pipe history--------

set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select 
(convert_timezone('Europe/London', s.start_time) )::timestamp_tz ,
(convert_timezone('Europe/London', s.end_time) )::timestamp_tz ,s.credits_used,s.bytes_inserted,s.files_inserted,s.pipe_id,s.pipe_name,
p.pipe_schema as schema_name,p.pipe_catalog as database_name
from
        snowflake.account_usage.pipe_usage_history s
        left join snowflake.account_usage.pipes p on
            p.pipe_id = s.pipe_id
                   WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(s.start_time) from pipe_usage_history s  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from PIPE_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', s.start_time) )::timestamp_tz ,
(convert_timezone('Europe/London', s.end_time) )::timestamp_tz ,s.credits_used,s.bytes_inserted,s.files_inserted,s.pipe_id,s.pipe_name,
p.pipe_schema as schema_name,p.pipe_catalog as database_name
from
        snowflake.account_usage.pipe_usage_history s
        left join snowflake.account_usage.pipes p on
            p.pipe_id = s.pipe_id
  WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(s.start_time) from pipe_usage_history s  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,CREDITS_USED,BYTES_INSERTED,FILES_INSERTED,PIPE_ID,PIPE_NAME,SCHEMA_NAME,DATABASE_NAME 
 from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.PIPE_USAGE_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));





set delta_count_usage=(select count(*) from
(select
 (convert_timezone('Europe/London', s.start_time) )::timestamp_tz ,
(convert_timezone('Europe/London', s.end_time) )::timestamp_tz ,s.credits_used,s.bytes_inserted,s.files_inserted,s.pipe_id,s.pipe_name,
p.pipe_schema as schema_name,p.pipe_catalog as database_name
from
        snowflake.account_usage.pipe_usage_history s
        left join snowflake.account_usage.pipes p on
            p.pipe_id = s.pipe_id
where convert_timezone('Europe/London',s.start_time)::timestamp_ntz>
(select ifnull( max( s.start_time ) , '2020-01-01' ) from pipe_usage_history s
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',s.start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.PIPE_USAGE_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from ( 
select (convert_timezone('Europe/London', s.start_time) )::timestamp_tz ,
(convert_timezone('Europe/London', s.end_time) )::timestamp_tz ,s.credits_used,s.bytes_inserted,s.files_inserted,s.pipe_id,s.pipe_name,
p.pipe_schema as schema_name,p.pipe_catalog as database_name
from
        snowflake.account_usage.pipe_usage_history s
        left join snowflake.account_usage.pipes p on
            p.pipe_id = s.pipe_id
where convert_timezone('Europe/London',s.start_time)::timestamp_ntz>
(select ifnull( max( s.start_time ) , '2020-01-01' ) from pipe_usage_history s where s.dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',s.start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select START_TIME,END_TIME,CREDITS_USED,BYTES_INSERTED,FILES_INSERTED,PIPE_ID,PIPE_NAME,SCHEMA_NAME,DATABASE_NAME 
from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.PIPE_USAGE_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));

--Print Result----

--Expected Count for PIPE_USAGE FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for PIPE_USAGE (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for PIPE_USAGE (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for PIPE_USAGE (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for PIPE_USAGE_HISTORY
select $delta_count_usage;

--Actual count for PIPE_USAGE_HISTORY 
select $delta_count_hist;

--Record Count Matches for PIPE_USAGE_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for PIPE_USAGE_HISTORY 
select $Unmatched_data;



-----------------------Replication history--------


set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz ,
DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED                       
  from snowflake.account_usage.REPLICATION_USAGE_HISTORY
 WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from REPLICATION_USAGE_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

 set delta_count_stage=( select count(*) from REPLICATION_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz ,
DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED from
SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
 WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from REPLICATION_USAGE_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select  START_TIME,END_TIME,DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_USAGE_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));




set delta_count_usage=(select count(*) from
(select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz ,
DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED
 from SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from REPLICATION_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_USAGE_HISTORY where
 convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date
 ));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz ,
DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED
 from SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from REPLICATION_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select START_TIME,END_TIME,DATABASE_ID,DATABASE_NAME,CREDITS_USED,BYTES_TRANSFERRED from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_USAGE_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date))
                    );

--Print Result----

--Expected Count for REPLICATION_USAGE FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for REPLICATION_USAGE (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for REPLICATION_USAGE (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for REPLICATION_USAGE (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for REPLICATION_USAGE_HISTORY
select $delta_count_usage;

--Actual count for REPLICATION_USAGE_HISTORY 
select $delta_count_hist;

--Record Count Matches for REPLICATION_USAGE_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for REPLICATION_USAGE_HISTORY 
select $Unmatched_data;


-------------------------------Search optimization History-----------------


set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (
    select
 (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME                      
  from snowflake.account_usage.SEARCH_OPTIMIZATION_HISTORY
                  WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from SEARCH_OPTIMIZATION_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from SEARCH_OPTIMIZATION_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from
SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
  WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from SEARCH_OPTIMIZATION_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select  START_TIME,END_TIME,CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SEARCH_OPTIMIZATION_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));
   





set delta_count_usage=(select count(*) from
(select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from SEARCH_OPTIMIZATION_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SEARCH_OPTIMIZATION_HISTORY 
 where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from SEARCH_OPTIMIZATION_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select START_TIME,END_TIME,CREDITS_USED,TABLE_ID,TABLE_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SEARCH_OPTIMIZATION_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));

--Print Result----

--Expected Count for SEARCH OPTIMIZATION  from Snowflake Account Usage
select $delta_count_usage_stg;

--Actual count for SEARCH OPTIMIZATION (STAGE Table)
select $delta_count_stage;

-- Record Count matches for SEARCH OPTIMIZATION  (STAGE Table)
select $Unmatched_record_stage;

--No of records failing data comparison for SEARCH OPTIMIZATION (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for SEARCH_OPTIMIZATION_HISTORY
select $delta_count_usage;

--Actual count for SEARCH_OPTIMIZATION_HISTORY 
select $delta_count_hist;

--Record Count Matches for SEARCH_OPTIMIZATION_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for SEARCH_OPTIMIZATION_HISTORY 
select $Unmatched_data;


--------------------------Stage_Storage_History-----------

set delta_count_usage_stg=(
SELECT COUNT(*)FROM
(select  convert_timezone('Europe/London', usage_date)::date,AVERAGE_STAGE_BYTES from
 snowflake.account_usage.STAGE_STORAGE_USAGE_HISTORY
 where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from STAGE_STORAGE_USAGE_HISTORY where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
));
 
 
 set delta_count_stage=(select count(*) from STAGE_STORAGE_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);


 set Unmatched_data_stage=(select count(*) from(
select  convert_timezone('Europe/London', usage_date)::date,
AVERAGE_STAGE_BYTES
        FROM snowflake.account_usage.STAGE_STORAGE_USAGE_HISTORY
 where convert_timezone('Europe/London',usage_date)::date>=
(select  ifnull( dateadd( hour, -4, max( usage_date ) ), '2020-01-01' ) from STAGE_STORAGE_USAGE_HISTORY where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)
 and convert_timezone('Europe/London',usage_date)::date<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select USAGE_DATE,AVERAGE_STAGE_BYTES from
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.STAGE_STORAGE_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));                    




set delta_count_usage=(select count(*) from
(select  convert_timezone('Europe/London', usage_date)::date,AVERAGE_STAGE_BYTES
 from SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY 
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from STAGE_STORAGE_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
));


set delta_count_hist=(select count(*) from STAGE_STORAGE_USAGE_HISTORY where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);


set Unmatched_data=(select count(*) from 
(select convert_timezone('Europe/London', usage_date)::date,AVERAGE_STAGE_BYTES
 from SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY
where convert_timezone('Europe/London',usage_date)::timestamp_ntz>
(select ifnull( max( usage_Date ) , '2020-01-01' ) from STAGE_STORAGE_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',usage_date)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
MINUS
select  USAGE_DATE,AVERAGE_STAGE_BYTES from
 SNO_CENTRAL_MONITORING_RAW_DB.LANDING.STAGE_STORAGE_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));


--Print Result----

--Expected Count for STAGE_STORAGE_USAGE from Snowflake Account Usage
select $delta_count_usage_stg;

--Actual count for STAGE_STORAGE_USAGE (STAGE Table)
select $delta_count_stage;

--Expected Count for STAGE_STORAGE_USAGE (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for STAGE_STORAGE_USAGE (STAGE TABLE)
select $Unmatched_data_stage;


--Expected Count for STAGE_STORAGE_USAGE_HISTORY
select $delta_count_usage;

--Actual count for STAGE_STORAGE_USAGE_HISTORY
select $delta_count_hist;

--Record Count Matches for STAGE_STORAGE_USAGE_HISTORY
select $Unmatched_records;

--No of records failing data comparison for STAGE_STORAGE_USAGE_HISTORY 
select $Unmatched_data;



---------------warehouse_metering_History----

set delta_count_usage_stg=(
  SELECT COUNT(*) from (
select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES FROM  snowflake.account_usage.WAREHOUSE_METERING_HISTORY
                   WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from WAREHOUSE_METERING_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
));

set delta_count_stage=( select count(*) from WAREHOUSE_METERING_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (

select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES from
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE convert_timezone('Europe/London',start_time)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(start_time) from WAREHOUSE_METERING_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select  START_TIME,END_TIME,WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.WAREHOUSE_METERING_HISTORY_stg
where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));


set delta_count_usage=(select count(*) from
(select   (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES
 from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from WAREHOUSE_METERING_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.WAREHOUSE_METERING_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select   (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES
 from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
where convert_timezone('Europe/London',start_time)::timestamp_ntz>
(select ifnull( max( start_time ) , '2020-01-01' ) from WAREHOUSE_METERING_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',start_time)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select  START_TIME,END_TIME,WAREHOUSE_ID,WAREHOUSE_NAME,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES from 
SNO_CENTRAL_MONITORING_RAW_DB.LANDING.WAREHOUSE_METERING_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)));

--Print Result----

--Expected Count for WAREHOUSE METERING  from Snowflake Account Usage

select $delta_count_usage_stg;

--Actual count for WAREHOUSE METERING (STAGE TABLE)
select $delta_count_stage;

--Record Count matches for WAREHOUSE METERING (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for WAREHOUSE METERING STAGE
select $Unmatched_data_stage;


--Expected Count for WAREHOUSE_METERING_HISTORY
select $delta_count_usage;

--Actual count for WAREHOUSE_METERING_HISTORY 
select $delta_count_hist;

--Record Count Matches for WAREHOUSE_METERING_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for WAREHOUSE_METERING_HISTORY 
select $Unmatched_data;


---------------------Serverless--Task--History-----------------------------------------------------------

set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME                     
  from snowflake.account_usage.SERVERLESS_TASK_HISTORY
                    WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from SERVERLESS_TASK_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from SERVERLESS_TASK_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME from
SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
 WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from SERVERLESS_TASK_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SERVERLESS_TASK_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));





set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from SERVERLESS_TASK_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SERVERLESS_TASK_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from SERVERLESS_TASK_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select  START_TIME,END_TIME,CREDITS_USED,TASK_ID,TASK_NAME,SCHEMA_ID,SCHEMA_NAME,DATABASE_ID,DATABASE_NAME
from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.SERVERLESS_TASK_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));

--Print Result----

--Expected Count for SERVERLESS_TASK_HISTORY FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for SERVERLESS_TASK_HISTORY (STAGE TABLE)
select $delta_count_stage;

---Record Count matches for SERVERLESS_TASK_HISTORY (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for SERVERLESS_TASK_HISTORY (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for SERVERLESS_TASK_HISTORY
select $delta_count_usage;

--Actual count for SERVERLESS_TASK_HISTORY 
select $delta_count_hist;

--Record Count Matches for SERVERLESS_TASK_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for SERVERLESS_TASK_HISTORY
select $Unmatched_data;


------------------------------------QUERY_ACCELERATION_HISTORY-------------------------------------------------------------

  set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME                     
  from snowflake.account_usage.QUERY_ACCELERATION_HISTORY
                    WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from QUERY_ACCELERATION_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from QUERY_ACCELERATION_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME from
SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY
 WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from QUERY_ACCELERATION_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME
 from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.QUERY_ACCELERATION_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));





set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from QUERY_ACCELERATION_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.QUERY_ACCELERATION_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME
 from SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from QUERY_ACCELERATION_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select  START_TIME,END_TIME,CREDITS_USED,WAREHOUSE_ID,WAREHOUSE_NAME
from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.QUERY_ACCELERATION_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));

--Print Result----

--Expected Count for QUERY_ACCELERATION_HISTORY FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for QUERY_ACCELERATION_HISTORY (STAGE TABLE)
select $delta_count_stage;

---Record Count matches for QUERY_ACCELERATION_HISTORY (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for QUERY_ACCELERATION_HISTORY (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for QUERY_ACCELERATION_HISTORY
select $delta_count_usage;

--Actual count for QUERY_ACCELERATION_HISTORY 
select $delta_count_hist;

--Record Count Matches for QUERY_ACCELERATION_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for QUERY_ACCELERATION_HISTORY
select $Unmatched_data;
                    
                    
----------------------------------REPLICATION_GROUP_USAGE_HISTORY----------------------------------------------------------------------------

set delta_count_usage_stg=(
  SELECT COUNT(*) FROM (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED                     
  from snowflake.account_usage.REPLICATION_GROUP_USAGE_HISTORY
                    WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from REPLICATION_GROUP_USAGE_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
  )); 
   

set delta_count_stage=( select count(*) from REPLICATION_GROUP_USAGE_HISTORY_STG where
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_record_stage=(select $delta_count_usage_stg=$delta_count_stage);

set Unmatched_data_stage=(select count(*) from (select
(convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED from
SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY
 WHERE convert_timezone('Europe/London',END_TIME)::timestamp_ntz >= 
   (select to_timestamp(select ifnull(dateadd(hour,-4,
                                          (select max(END_TIME) from REPLICATION_GROUP_USAGE_HISTORY  where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
                                          )    )
 ,'2020-01-01'                       ) 
                       )
   ) and convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp()::date)
 MINUS
 select START_TIME,END_TIME,REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED
 from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_GROUP_USAGE_HISTORY_STG
where 
convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
  ));





set delta_count_usage=(select count(*) from
(select  (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED
 from SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from REPLICATION_GROUP_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
 ));

set delta_count_hist=(select count(*) from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_GROUP_USAGE_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date));

set Unmatched_records=(select $delta_count_usage=$delta_count_hist);

set Unmatched_data=(select count(*) from 
 (select (convert_timezone('Europe/London', START_TIME) )::timestamp_tz,(convert_timezone('Europe/London', END_TIME) )::timestamp_tz,
REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED
 from SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY 
where convert_timezone('Europe/London',END_TIME)::timestamp_ntz>
(select ifnull( max( END_TIME ) , '2020-01-01' ) from REPLICATION_GROUP_USAGE_HISTORY
 where dw_load_ts<
 convert_timezone('Europe/London',current_timestamp()::date)
)and
convert_timezone('Europe/London',END_TIME)::timestamp_ntz<convert_timezone('Europe/London',current_timestamp())::date
                    Minus
select  START_TIME,END_TIME,REPLICATION_GROUP_NAME,REPLICATION_GROUP_ID,CREDITS_USED,BYTES_TRANSFERRED
from SNO_CENTRAL_MONITORING_RAW_DB.LANDING.REPLICATION_GROUP_USAGE_HISTORY
where convert_timezone('Europe/London',dw_load_ts::date)>=convert_timezone('Europe/London',current_timestamp()::date)
));

--Print Result----

--Expected Count for REPLICATION_GROUP_USAGE_HISTORY FROM SNOFALKE ACCOUNT USAGE
select $delta_count_usage_stg;

--Actual count for REPLICATION_GROUP_USAGE_HISTORY (STAGE TABLE)
select $delta_count_stage;

---Record Count matches for REPLICATION_GROUP_USAGE_HISTORY (STAGE TABLE)
select $Unmatched_record_stage;

--No of records failing data comparison for REPLICATION_GROUP_USAGE_HISTORY (STAGE Table)
select $Unmatched_data_stage;


--Expected Count for REPLICATION_GROUP_USAGE_HISTORY
select $delta_count_usage;

--Actual count for REPLICATION_GROUP_USAGE_HISTORY 
select $delta_count_hist;

--Record Count Matches for REPLICATION_GROUP_USAGE_HISTORY 
select $Unmatched_records;

--No of records failing data comparison for REPLICATION_GROUP_USAGE_HISTORY
select $Unmatched_data;
                    
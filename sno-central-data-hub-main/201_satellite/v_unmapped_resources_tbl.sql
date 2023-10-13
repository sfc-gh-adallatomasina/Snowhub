--------------------------------------------------------------------
--  Purpose: create tables for unmapped_resources
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  21-03-2023 sayali phadtare table for storing vnmapped_resources
--------------------------------------------------------------------


--
-- permanent history table with retention days
--
create table if not exists &{l_target_db}_DATA_REP.&{l_target_schema}.unmapped_resources
(
     ORGANIZATION_NAME VARCHAR(100) NULL,
     ACCOUNT_NAME VARCHAR(100) NOT NULL,
     REGION_NAME VARCHAR(100) NOT NULL,
     RESOURCE_NAME VARCHAR(100) NULL,
     RESOURCE_TYPE_CD VARCHAR(100) NOT NULL
)
data_retention_time_in_days = 1
;


ALTER TABLE &{l_target_db}_DATA_REP.&{l_target_schema}.unmapped_resources ALTER COLUMN RESOURCE_NAME DROP NOT NULL;
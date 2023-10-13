--  Purpose: create task for unmapped resources
--
--  Revision History:
--  Date     Engineer           Description
--  -------- ------------- -------------------------------------------------------------------------------
-- 21/03/23	sayali phadtare 	v_mapped_resource for secondary database
-- 13/06/23 Nareesh Komuravelly Converted to serverless task
-----------------------------------------------------------------------------------------------------------

use role &{l_entity_name}_sysadmin&{l_fr_suffix};

use database &{l_target_db};
use schema &{l_target_schema};
use warehouse &{l_target_wh};

CREATE OR REPLACE TASK &{l_target_db}.&{l_target_schema}.V_UNMAPPED_TASK
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
--warehouse = '&{l_target_wh}'
SCHEDULE = '60 MINUTE' --This should be small for testing
AS

INSERT OVERWRITE INTO &{l_target_db}.&{l_target_schema}.UNMAPPED_RESOURCES
  SELECT * FROM &{l_target_db}.&{l_target_schema}.V_UNMAPPED_RESOURCES;


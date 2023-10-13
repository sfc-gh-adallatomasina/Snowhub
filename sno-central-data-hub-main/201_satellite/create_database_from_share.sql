--------------------------------------------------------------------
--  Purpose: Create database from SHARE
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 13/09/2022 	Alessandro Dallatomasina 	V0.1
--------------------------------------------------------------------

set l_db_name =  (select 'SAT_'||'&{l_satellite_account}');

CREATE DATABASE IF NOT EXISTS IDENTIFIER($l_db_name) FROM SHARE &{l_satellite_org_name}.&{l_satellite_account}.SATELLITE_SHARE COMMENT='&{l_satellite_org_name}&{l_satellite_account}_SNO_CENTRAL_MONITORING_SHARE';

GRANT IMPORTED PRIVILEGES ON DATABASE IDENTIFIER($l_db_name) TO ROLE &{l_entity_name}_sysadmin&{l_fr_suffix};




--------------------------------------------------------------------
--  Purpose: Deploy objects in the satellites envs
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  13/05/22 Alessandro Dallatomasina   First release
--  08/09/22 Alessandro Dallatomasina   Landing zone 2.0
--------------------------------------------------------------------

--Run the script using snowsql from the Chargeback directory:
--snowsql -c <env> -r sysadmin -D l_account_name=<account_name> -f chargebacks_account_uninstall.sql -o output_file=chargebacks_account_uninstall.out

------------------------------------------------------------------------------
-- snowsql session
--
!set variable_substitution=true
!set exit_on_error=True
!set results=true
!set echo=true

--------------------------------------------------------------------
-- environment definition
--
!print set environment definition

!define l_db_suffix=_db
!define l_wh_suffix=_wh
!define l_ar_suffix=_ar
!define l_fr_suffix=_fr

------------------------------------------------------------------------------
-- 000_admin
--
!print 000_admin

!print set context

!source accounts_config/&{l_account_name}_context.sql

!print show existing
show roles      like '&{l_entity_name}%';
show databases  like '&{l_target_db}%';
show schemas  like '&{l_target_schema}%';

show warehouses like '&{l_target_wh}%';

---------------------DROP SECURITYADMIN ROLE START-----------------------------
use role securityadmin;

-- keep create statement minimal
DROP role if exists &{l_entity_name}_securityadmin&{l_fr_suffix};
---------------------DROP SECURITYADMIN ROLE END-----------------------------

---------------------DROP SYSADMIM ROLE START-----------------------------
use role securityadmin;

-- keep create statement minimal
DROP role if exists &{l_entity_name}_sysadmin&{l_fr_suffix};
DROP role if exists &{l_entity_name}_OPS&{l_fr_suffix};
---------------------DROP SYSADMIM ROLE END-----------------------------

use role accountadmin;

---------------------DROP SHARE START-----------------------------

DROP SHARE IF EXISTS SATELLITE_SHARE;

---------------------DROP SHARE END-----------------------------

---------------------DROP DATABASE AND SCHEMA START-----------------------------

DROP DATABASE IF EXISTS &{l_target_db} CASCADE;

DROP DATABASE IF EXISTS &{l_target_db}_DATA_REP;
---------------------DROP DATABASE AND SCHEMA END-----------------------------

---------------------DROP WAREHOUSE START-----------------------------

DROP WAREHOUSE IF EXISTS &{l_target_wh};
 
---------------------DROP WAREHOUSE END-----------------------------


!print SUCCESS!
!quit

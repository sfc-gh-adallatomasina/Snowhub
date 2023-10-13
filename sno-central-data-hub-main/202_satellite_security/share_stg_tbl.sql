--  Purpose: stage table for show_shares
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
-- 26/05/2023	sayali phadtare 	show_shares 

----------------------------------------------------------------------------------------------------------- 
use role &{l_entity_name}_sysadmin&{l_fr_suffix};
use database &{l_target_db};
use schema &{l_sec_schema};
use warehouse &{l_target_wh};

CREATE TRANSIENT TABLE IF NOT EXISTS  &{l_target_db}.&{l_sec_schema}.shares_stg
(
ORGANIZATION_NAME                VARCHAR(250)   NOT NULL,
ACCOUNT_NAME                     VARCHAR(250)   NOT  NULL,
REGION_NAME                      VARCHAR(250)   NOT NULL,
CREATED                          TIMESTAMP_LTZ  NOT NULL,
SHARE_TYPE                       VARCHAR        NULL,
SHARE_NAME                       VARCHAR        NULL,
DATABASE_NAME                    VARCHAR        NULL,
SHARE_TO                         VARCHAR        NULL,
OWNER                            VARCHAR        NULL,
COMMENT                          VARCHAR        NULL,
LISTING_GLOBAL_NAME              VARCHAR        NULL,
DW_LOAD_TS                       TIMESTAMP_LTZ  NOT NULL
) data_retention_time_in_days = 1;

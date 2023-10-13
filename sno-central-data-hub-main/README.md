# Uninstall

Uninstall/Rollback needs to happen in the below sequence. Please be EXTRA CAUTIOUS before triggering the rollback, as it will drop databases in both Satellite and Hub accounts

## Drop satellite databases created via Share & Replicas in Hub Account

> Note: `-c` is hub account name and `-l_account_name` is satellite name

1) snowsql -c < hub-account-name > -r sysadmin -D l_env_type=dev -D l_account_name=< Satellite account_name > -f chargebacks_account_uninstall_hub_share.sql -o output_file=chargebacks_account_uninstall.out

## Rollback Satellite 

2) snowsql -c < Satellite account name > -r sysadmin -D l_env_type=dev -D l_account_name=< Satellite account_name > -f chargebacks_account_uninstall.sql o output_file=chargebacks_account_uninstall.out

# Rollback Hub 

1) snowsql -c < Hub Account name > -r sysadmin -D l_env_type=dev -D l_account_name=< Hub account_name > -f chargebacks_account_uninstall.sql o output_file=chargebacks_account_uninstall.out


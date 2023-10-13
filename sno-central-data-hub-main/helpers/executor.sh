# creates context file

echo "pwd from executor"
pwd

SNOWFLAKE_ACCOUNT_NAME=$(echo $1 | tr 'a-z' 'A-Z')
SNOWFLAKE_ACCOUNT_USER=$(echo $2 | tr 'a-z' 'A-Z')
SNOWFLAKE_ACCOUNT_KEY=$3
SNOWFLAKE_ACCOUNT_ORG_NAME=$(echo $4 | tr 'a-z' 'A-Z')
SNOWFLAKE_PRIVATE_LINK_USAGE=$(echo $5 | tr 'a-z' 'A-Z')
SNOWFLAKE_ACCOUNT_TYPE=$6
SNOWFLAKE_DEPLOY_ACTIVITY=$7

echo "All arguments passed to Executor are: $*"

context_file=../accounts_config/${SNOWFLAKE_ACCOUNT_NAME}_context.sql

if [ -f "$context_file" ]; then
    echo "$context_file file exists. Proceeding further"
    echo `pwd`
else 
    echo "$context_file does not exist. Creating new file"
    echo $context_file
    touch $context_file
    echo "!print set environment context" >> $context_file
    echo "!define l_target_db=SNO_CENTRAL_MONITORING_RAW_DB" >> $context_file
    echo "!define l_target_schema=LANDING" >> $context_file
    echo "!define l_sec_schema=SECURITY" >> $context_file
    echo "!define l_target_wh=SNO_CENTRAL_MONITORING_WH" >> $context_file
    echo "!define l_entity_name=SNO_CENTRAL_MONITORING" >> $context_file    
    echo "!define l_satellite_org_name=${SNOWFLAKE_SAT_ACCOUNT_ORG_NAME}" >> $context_file
    echo "!define l_satellite_account=${SNOWFLAKE_SAT_ACCOUNT_NAME}" >> $context_file 
    echo "!define l_satellite_region=${SNOWFLAKE_SAT_REGION}" >> $context_file       
    echo "!define l_hub_org_name=${SNOWFLAKE_ACCOUNT_ORG_NAME}" >> $context_file
    echo "!define l_hub_account=${SNOWFLAKE_HUB_ACCOUNT_NAME}" >> $context_file
    echo "!define l_hub_region=${SNOWFLAKE_HUB_REGION}" >> $context_file
    echo "!define l_cron='USING CRON 30 4 * * * Europe/London'" >> $context_file       
    echo "" >> $context_file
    echo "!print SUCCESS!" >> $context_file
fi

cat $context_file

#exit 1

ls -al ../accounts_config/
cd ../

echo "Begin Snowflake deployment to ${SNOWFLAKE_ACCOUNT_TYPE}... "
pwd
ls -l log/

#export SNOWSQL_PWD=${SNOWFLAKE_ACCOUNT_PASSWORD}
#echo "Password for Snowflake account is: ${SNOWSQL_PWD}"

#echo "Key: ${SVC_TF_SF_USER}"
echo "Key: ${!SNOWFLAKE_ACCOUNT_USER}"
export SNOWSQL_PRIVATE_KEY_PASSPHRASE=${SVC_TF_SF_USER_PRIVATE_KEY_PASSPHRASE}

if [ ${SNOWFLAKE_ACCOUNT_TYPE} == "SATELLITE" ] && [ ${SNOWFLAKE_DEPLOY_ACTIVITY} == "DEPLOY_SHARE" ]; then
  SCRIPT_NAME="chargebacks_account_satellite_deploy_share.sql"
  LOGFILE_NAME="/builds/app/app-01434/applications/sno-central-data-hub/log/chargebacks_sat_account_deploy_share_${SNOWFLAKE_ACCOUNT_NAME}.out"
elif [ ${SNOWFLAKE_ACCOUNT_TYPE} == "SATELLITE" ] && [ ${SNOWFLAKE_DEPLOY_ACTIVITY} == "DEPLOY_REPLICATION" ]; then
  SCRIPT_NAME="chargebacks_account_satellite_deploy_rep.sql"
  LOGFILE_NAME="/builds/app/app-01434/applications/sno-central-data-hub/log/chargebacks_sat_account_deploy_rep_${SNOWFLAKE_ACCOUNT_NAME}.out" 
elif [ ${SNOWFLAKE_ACCOUNT_TYPE} == "HUB" ] && [ ${SNOWFLAKE_DEPLOY_ACTIVITY} == "INITIALISE" ]; then
  SCRIPT_NAME="chargebacks_account_hub_initialise.sql"
  LOGFILE_NAME="/builds/app/app-01434/applications/sno-central-data-hub/log/chargebacks_hub_account_initialise_${SNOWFLAKE_ACCOUNT_NAME}.out"
elif [ ${SNOWFLAKE_ACCOUNT_TYPE} == "HUB" ] && [ ${SNOWFLAKE_DEPLOY_ACTIVITY} == "DEPLOY_SHARE" ]; then
  SCRIPT_NAME="chargebacks_satellite_hub_share.sql"
  LOGFILE_NAME="/builds/app/app-01434/applications/sno-central-data-hub/log/chargebacks_satellite_hub_share_${SNOWFLAKE_ACCOUNT_NAME}.out"
elif [ ${SNOWFLAKE_ACCOUNT_TYPE} == "HUB" ] && [ ${SNOWFLAKE_DEPLOY_ACTIVITY} == "DEPLOY_REPLICATION" ]; then
  SCRIPT_NAME="chargebacks_satellite_hub_rep.sql"
  LOGFILE_NAME="/builds/app/app-01434/applications/sno-central-data-hub/log/chargebacks_satellite_hub_rep_${SNOWFLAKE_ACCOUNT_NAME}.out"
fi

if [ ${SNOWFLAKE_PRIVATE_LINK_USAGE} == "YES" ]; then
  ACCT_NAME="lseg-${SNOWFLAKE_ACCOUNT_NAME}.privatelink"
  #ACCT_NAME="lseg-${SNOWFLAKE_ACCOUNT_NAME}"
else
  ACCT_NAME="lseg-${SNOWFLAKE_ACCOUNT_NAME}"
fi

echo "Snowflake Account is: ${SNOWFLAKE_ACCOUNT_NAME}"
echo "Snowflake Account used for connecting: ${ACCT_NAME}"
echo "Snowflake deploy activity is: ${SNOWFLAKE_DEPLOY_ACTIVITY}"
echo "Snowflake Account Type is: ${SNOWFLAKE_ACCOUNT_TYPE}"
echo "Script name is: ${SCRIPT_NAME}"
echo "logfile name is: ${LOGFILE_NAME}"
echo "Calling Snowsql"

#snowsql -a lseg-${SNOWFLAKE_ACCOUNT_NAME} -u ${SNOWFLAKE_ACCOUNT_USER} -r sysadmin -D l_account_name=${SNOWFLAKE_ACCOUNT_NAME} -f ${SCRIPT_NAME} -o output_file=${LOGFILE_NAME}
# set proxy - just a workaround

export NO_PROXY=='.r53,.local,bitbucket.unix.lch.com,.stockex.local,169.254,172.16,100.64,10.192.0.0,192.168,10.,nexus.lseg.stockex.local,.privatelink.snowflakecomputing.com,monitoring.eu-west-2.amazonaws.com,kms.eu-west-2.amazonaws.com,s3.eu-west-2.amazonaws.com,logs.eu-west-2.amazonaws.com,ec2messages.eu-west-2.amazonaws.com,ec2.eu-west-2.amazonaws.com,ssmmessages.eu-west-2.amazonaws.com,ssm.eu-west-2.amazonaws.com'

snowsql -v
snowsql -a ${ACCT_NAME} -u ${SNOWFLAKE_ACCOUNT_USER} --private-key-path ${!SNOWFLAKE_ACCOUNT_KEY} -D l_account_name=${SNOWFLAKE_ACCOUNT_NAME} -f ${SCRIPT_NAME} -o output_file=${LOGFILE_NAME}

retval=$?
if [ ${retval} -eq 0 ]; then
  echo "Deployment successful... "
else 
  echo "Deployment Failed, exiting... "
  cat ${LOGFILE_NAME}
  exit 1
fi

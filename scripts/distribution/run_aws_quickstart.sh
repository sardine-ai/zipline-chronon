#!/bin/bash
set -xo pipefail

# default option is to not create a new cluster for testing but use existing cluster
create_cluster=false

# otherwise use the canary cluster id
CANARY_CLUSTER_ID="j-13BASWFP15TLR"

ENVIRONMENT="dev"

# Loop through all arguments
for arg in "$@"; do
  if [ "$arg" = "--create-cluster" ]; then
    create_cluster=true
  fi
  if [[ "$arg" == "--canary" ]]; then
    ENVIRONMENT="canary"
  fi
done

# Delete glue tables
# hardcoding the s3 path here because that's where the underlying location of the data is for this glue database `data`
# Faster to just aws s3 rm than to aws glue delete-table-version + delete-partition and then finally delete-table
if [[ "$ENVIRONMENT" == "canary" ]]; then
  aws s3 rm s3://zipline-warehouse-canary/data/aws_purchases_v1_test --recursive
  aws glue delete-table --database-name data --name quickstart_purchases_v1_test
else
  aws s3 rm s3://zipline-warehouse-dev/data/aws_purchases_v1_dev --recursive
  aws glue delete-table --database-name data --name quickstart_purchases_v1_dev
fi

#aws s3 rm s3://zipline-warehouse-canary/data/aws_plaid_fv_v1 --recursive
#aws glue delete-table --database-name data --name aws_plaid_fv_v1


# Sleep for a bit since aws glue delete-table is asynchronous
sleep 30

GREEN='\033[0;32m'
RED='\033[0;31m'


#git fetch origin davidhan/run_aws
#git checkout davidhan/run_aws


# Create a virtualenv to fresh install zipline-ai
VENV_DIR="tmp_chronon"
rm -rf $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Download the wheel
if [[ "$ENVIRONMENT" == "canary" ]]; then
  WHEEL_FILE=$(aws s3 ls s3://zipline-artifacts-canary/release/candidate/wheels/ | grep zipline_ai | sort | tail -n 1 | awk '{print $4}')
  aws s3 cp s3://zipline-artifacts-canary/release/candidate/wheels/$WHEEL_FILE .
else
  WHEEL_FILE=$(aws s3 ls s3://zipline-artifacts-dev/release/latest/wheels/ | grep zipline_ai | sort | tail -n 1 | awk '{print $4}')
  aws s3 cp s3://zipline-artifacts-dev/release/latest/wheels/$WHEEL_FILE .
fi

# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE

export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# function to check dataproc job id state
function check_dataproc_job_state() {
  JOB_ID=$1
  if [ -z "$JOB_ID" ]; then
        echo "No job id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  JOB_STATE=$(gcloud dataproc jobs describe $JOB_ID --region=us-central1 --format=flattened | grep "status.state:")
  echo $JOB_STATE
#  TODO: this doesn't actually fail. need to fix.
  if [ -z "$JOB_STATE" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
}

function check_emr_step_state() {
  CHECK_STEP_ID=$1
  CHECK_CLUSTER_ID=$2
  if [ -z "$CHECK_STEP_ID" ]; then
        echo "No step id available to check. Exiting."
        exit 1
  fi
  if [ -z "$CHECK_CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  # check exit code
  if ! aws emr wait step-complete --cluster-id $CHECK_CLUSTER_ID --step-id $CHECK_STEP_ID; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi

  STEP_STATE=$(aws emr describe-step --cluster-id $CHECK_CLUSTER_ID --step-id $CHECK_STEP_ID --query Step.Status.State | tr -d '"')
  if [ "$STEP_STATE" != "COMPLETED" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
  echo "succeeded"
}

function check_emr_cluster_state() {
  CHECK_CLUSTER_ID=$1
  if [ -z "$CHECK_CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  if ! aws emr wait cluster-running --cluster-id $CHECK_CLUSTER_ID; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------CLUSTER CREATION FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
  aws emr describe-cluster --cluster-id $CHECK_CLUSTER_ID
}


CHRONON_ROOT=`pwd`/api/python/test/canary
export PYTHONPATH="${PYTHONPATH}:$CHRONON_ROOT"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline compile -chronon_root=$CHRONON_ROOT --conf=group_bys/aws/purchases.py
else
  zipline compile -chronon_root=$CHRONON_ROOT --conf=group_bys/aws/purchases.py
fi

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out

if [ "$create_cluster" = true ]; then
  echo "Creating a new EMR cluster"
  if [[ "$ENVIRONMENT" == "canary" ]]; then
    zipline run --repo=$CHRONON_ROOT --mode backfill --conf production/group_bys/aws/purchases.v1_test --end-ds 20250220 --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
  else
    zipline run --repo=$CHRONON_ROOT --mode backfill --conf production/group_bys/aws/purchases.v1_dev --end-ds 20250220 --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
  fi
  EMR_SUBMITTER_ID_CLUSTER_STR="EMR job id"
  CLUSTER_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITTER_ID_CLUSTER_STR"  | cut -d " " -f4) # expecting the cluster id to be the 4th field
  check_emr_cluster_state $CLUSTER_ID
  # Get the step id
  STEP_ID=$(aws emr list-steps --cluster-id $CLUSTER_ID | jq -r '.Steps[0].Id')
  check_emr_step_state $STEP_ID $CLUSTER_ID
else
  CLUSTER_ID=$CANARY_CLUSTER_ID
  echo "Using existing EMR cluster $CLUSTER_ID"
  if [[ "$ENVIRONMENT" == "canary" ]]; then
    EMR_CLUSTER_ID=$CLUSTER_ID zipline run --repo=$CHRONON_ROOT --mode backfill --conf production/group_bys/aws/purchases.v1_test --end-ds 20250220 2>&1 | tee tmp_backfill.out
  else
    EMR_CLUSTER_ID=$CLUSTER_ID zipline run --repo=$CHRONON_ROOT --mode backfill --conf production/group_bys/aws/purchases.v1_dev --end-ds 20250220 2>&1 | tee tmp_backfill.out
  fi

  EMR_SUBMITER_ID_STEP_STR="EMR step id"
  STEP_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITER_ID_STEP_STR"  | cut -d " " -f4) # expecting the step id to be the 4th field
  check_emr_step_state $STEP_ID $CLUSTER_ID
fi


echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

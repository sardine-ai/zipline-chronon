#!/bin/bash
set -xo pipefail

# default option is to not create a new cluster for testing but use existing cluster
create_cluster=false

# otherwise use the canary cluster id
CANARY_CLUSTER_ID="j-13BASWFP15TLR"

# Loop through all arguments
for arg in "$@"; do
  if [ "$arg" = "--create-cluster" ]; then
    create_cluster=true
    break
  fi
done

mkdir -p /tmp/zipline_test/aws
cd /tmp/zipline_test/aws
rm -rf cananry-confs

# Delete glue tables
# hardcoding the s3 path here because that's where the underlying location of the data is for this glue database `data`
# Faster to just aws s3 rm than to aws glue delete-table-version + delete-partition and then finally delete-table
aws s3 rm s3://zipline-warehouse-canary/data/quickstart_purchases_v1_dev --recursive
aws glue delete-table --database-name data --name quickstart_purchases_v1_dev

# Sleep for a bit since aws glue delete-table is asynchronous
sleep 30

GREEN='\033[0;32m'
RED='\033[0;31m'

WHEEL_FILE="zipline_ai-0.1.0-py3-none-any.whl "

# Clone the cananry configs
git clone git@github.com:zipline-ai/cananry-confs.git
cd cananry-confs

# Use the branch with Zipline specific team.json
git fetch origin davidhan/run_aws
git checkout davidhan/run_aws

# Create a virtualenv to fresh install zipline-ai
python3 -m venv tmp_chronon
source tmp_chronon/bin/activate

# Download the wheel
aws s3 cp s3://zipline-artifacts-canary/jars/$WHEEL_FILE .

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

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --conf=group_bys/quickstart/purchases.py

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out

if [ "$create_cluster" = true ]; then
  echo "Creating a new EMR cluster"
  zipline run --conf production/group_bys/quickstart/purchases.v1_dev --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
  EMR_SUBMITTER_ID_CLUSTER_STR="EMR job id"
  CLUSTER_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITTER_ID_CLUSTER_STR"  | cut -d " " -f4) # expecting the cluster id to be the 4th field
  check_emr_cluster_state $CLUSTER_ID
  # Get the step id
  STEP_ID=$(aws emr list-steps --cluster-id $CLUSTER_ID | jq -r '.Steps[0].Id')
  check_emr_step_state $STEP_ID $CLUSTER_ID
else
  CLUSTER_ID=$CANARY_CLUSTER_ID
  echo "Using existing EMR cluster $CLUSTER_ID"
  EMR_CLUSTER_ID=$CLUSTER_ID zipline run --conf production/group_bys/quickstart/purchases.v1_dev 2>&1 | tee tmp_backfill.out
  EMR_SUBMITER_ID_STEP_STR="EMR step id"
  STEP_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITER_ID_STEP_STR"  | cut -d " " -f4) # expecting the step id to be the 4th field
  check_emr_step_state $STEP_ID $CLUSTER_ID
fi


echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

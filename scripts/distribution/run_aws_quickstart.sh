#!/bin/bash
# Run this bash script via the python file `distribution/run_zipline_quickstart.py`
set -xo pipefail

mkdir /tmp/zipline/aws/
rm -rf /tmp/zipline/aws/cananry-confs
cd /tmp/zipline/aws/


GREEN='\033[0;32m'
RED='\033[0;31m'

WHEEL_FILE="zipline_ai-0.1.0-py3-none-any.whl "

# Delete glue tables to start from scratch
#bq rm -f -t canary-443022:data.quickstart_purchases_v1_test
#TODO: delete bigtable rows

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

function check_emr_step_state() {
  STEP_ID=$1
  CLUSTER_ID=$2
  if [ -z "$STEP_ID" ]; then
        echo "No step id available to check. Exiting."
        exit 1
  fi
  if [ -z "$CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  # check exit code
  if ! aws emr wait step-complete --cluster-id $CLUSTER_ID --step-id $STEP_ID; then
        echo "Job failed"
        exit 1
  fi
  aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID
}

function check_emr_cluster_state() {
  CLUSTER_ID=$1
  if [ -z "$CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  if ! aws emr wait cluster-running --cluster-id $CLUSTER_ID; then
        echo "Cluster failed"
        exit 1
  fi
  aws emr describe-cluster --cluster-id $CLUSTER_ID
}



EMR_SUBMITTER_ID_CLUSTER_STR="EMR job id"
EMR_SUBMITER_ID_STEP_STR="EMR step id"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --conf=group_bys/quickstart/purchases.py

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out

CLUSTER_ID=j-13BASWFP15TLR zipline run --conf production/group_bys/quickstart/purchases.v1_dev 2>&1 | tee tmp_backfill.out

#zipline run --conf production/group_bys/quickstart/purchases.v1_dev --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
#CLUSTER_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITTER_ID_CLUSTER_STR"  | cut -d " " -f5)
#check_emr_cluster_state $CLUSTER_ID
#
## Get the step id
#STEP_ID=$(aws emr list-steps --cluster-id $CLUSTER_ID | jq -r '.Steps[0].Id')
#check_emr_step_state $STEP_ID $CLUSTER_ID




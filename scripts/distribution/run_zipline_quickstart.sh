#!/bin/bash
# Run this bash script via the python file `distribution/run_zipline_quickstart.py`
set -xo pipefail



WORKING_DIR=$1
cd $WORKING_DIR


GREEN='\033[0;32m'
RED='\033[0;31m'

WHEEL_FILE="zipline_ai-0.1.0-py3-none-any.whl "

# Delete gcp tables to start from scratch
bq rm -f -t canary-443022:data.quickstart_purchases_v1_test
bq rm -f -t canary-443022:data.quickstart_purchases_v1_test_upload
#TODO: delete bigtable rows

# Clone the cananry configs
git clone git@github.com:zipline-ai/cananry-confs.git
cd cananry-confs

# Use the branch with Zipline specific team.json
git fetch origin davidhan/selects
git checkout davidhan/selects

# Create a virtualenv to fresh install zipline-ai
python3 -m venv tmp_chronon
source tmp_chronon/bin/activate

# Download the wheel
gcloud storage cp gs://zipline-artifacts-canary/jars/$WHEEL_FILE .

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
        echo "Job failed"
        exit 1
  fi
}

DATAPROC_SUBMITTER_ID_STR="Dataproc submitter job id"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --conf=group_bys/quickstart/purchases.py

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out
zipline run --conf production/group_bys/quickstart/purchases.v1_test --dataproc 2>&1 | tee tmp_backfill.out
BACKFILL_JOB_ID=$(cat tmp_backfill.out | grep "$DATAPROC_SUBMITTER_ID_STR"  | cut -d " " -f5)
check_dataproc_job_state $BACKFILL_JOB_ID

echo -e "${GREEN}<<<<<.....................................GROUP-BY-UPLOAD.....................................>>>>>\033[0m"
touch tmp_gbu.out
zipline run --mode upload --conf production/group_bys/quickstart/purchases.v1_test --ds  2023-12-01 --dataproc 2>&1 | tee tmp_gbu.out
GBU_JOB_ID=$(cat tmp_gbu.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $GBU_JOB_ID

# Need to wait for upload to finish
echo -e "${GREEN}<<<<<.....................................UPLOAD-TO-KV.....................................>>>>>\033[0m"
touch tmp_upload_to_kv.out
zipline run --mode upload-to-kv --conf production/group_bys/quickstart/purchases.v1_test --partition-string=2023-12-01 --dataproc 2>&1 | tee tmp_upload_to_kv.out
UPLOAD_TO_KV_JOB_ID=$(cat tmp_upload_to_kv.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $UPLOAD_TO_KV_JOB_ID

echo -e "${GREEN}<<<<< .....................................METADATA-UPLOAD.....................................>>>>>\033[0m"
touch tmp_metadata_upload.out
zipline run --mode metadata-upload --conf production/group_bys/quickstart/purchases.v1_test --dataproc 2>&1 | tee tmp_metadata_upload.out
METADATA_UPLOAD_JOB_ID=$(cat tmp_metadata_upload.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $METADATA_UPLOAD_JOB_ID

# Need to wait for upload-to-kv to finish
echo -e "${GREEN}<<<<<.....................................FETCH.....................................>>>>>\033[0m"
touch tmp_fetch.out
zipline run --mode fetch --conf-type group_bys --name quickstart.purchases.v1_test -k '{"user_id":"5"}' --gcp 2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
cat tmp_fetch.out | grep purchase_price_average_14d
# check if exit code of previous is 0
if [ $? -ne 0 ]; then
  echo "Failed to find purchase_price_average_14d"
  exit 1
fi

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"


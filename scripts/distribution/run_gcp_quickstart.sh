#!/bin/bash
set -xo pipefail


GREEN='\033[0;32m'
RED='\033[0;31m'

WHEEL_FILE="zipline_ai-0.1.0-py3-none-any.whl "

# Delete gcp tables to start from scratch
bq rm -f -t canary-443022:data.gcp_purchases_v1_dev
bq rm -f -t canary-443022:data.gcp_purchases_v1_dev_upload
#TODO: delete bigtable rows

# Create a virtualenv to fresh install zipline-ai
VENV_DIR="tmp_chronon"
rm -rf $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Download the wheel
gcloud storage cp gs://zipline-artifacts-dev/jars/$WHEEL_FILE .

# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE


CHRONON_ROOT=`pwd`/api/py/test/canary
export PYTHONPATH="${PYTHONPATH}:$CHRONON_ROOT"


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

DATAPROC_SUBMITTER_ID_STR="Dataproc submitter job id"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --chronon_root=$CHRONON_ROOT --conf=group_bys/gcp/purchases.py

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out
zipline run --repo=$CHRONON_ROOT --mode backfill --conf production/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_backfill.out
BACKFILL_JOB_ID=$(cat tmp_backfill.out | grep "$DATAPROC_SUBMITTER_ID_STR"  | cut -d " " -f5)
check_dataproc_job_state $BACKFILL_JOB_ID

echo -e "${GREEN}<<<<<.....................................GROUP-BY-UPLOAD.....................................>>>>>\033[0m"
touch tmp_gbu.out
zipline run --repo=$CHRONON_ROOT --mode upload --conf production/group_bys/gcp/purchases.v1_dev --ds  2023-12-01 --dataproc 2>&1 | tee tmp_gbu.out
GBU_JOB_ID=$(cat tmp_gbu.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $GBU_JOB_ID

# Need to wait for upload to finish
echo -e "${GREEN}<<<<<.....................................UPLOAD-TO-KV.....................................>>>>>\033[0m"
touch tmp_upload_to_kv.out
zipline run --repo=$CHRONON_ROOT --mode upload-to-kv --conf production/group_bys/gcp/purchases.v1_dev --partition-string=2023-12-01 --dataproc 2>&1 | tee tmp_upload_to_kv.out
UPLOAD_TO_KV_JOB_ID=$(cat tmp_upload_to_kv.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $UPLOAD_TO_KV_JOB_ID

echo -e "${GREEN}<<<<< .....................................METADATA-UPLOAD.....................................>>>>>\033[0m"
touch tmp_metadata_upload.out
zipline run --repo=$CHRONON_ROOT --mode metadata-upload --conf production/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_metadata_upload.out
METADATA_UPLOAD_JOB_ID=$(cat tmp_metadata_upload.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $METADATA_UPLOAD_JOB_ID

# Need to wait for upload-to-kv to finish
echo -e "${GREEN}<<<<<.....................................FETCH.....................................>>>>>\033[0m"
touch tmp_fetch.out
zipline run --repo=$CHRONON_ROOT --mode fetch --conf=production/group_bys/gcp/purchases.v1_dev -k '{"user_id":"5"}' --name gcp.purchases.v1_dev 2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
cat tmp_fetch.out | grep purchase_price_average_14d
# check if exit code of previous is 0
if [ $? -ne 0 ]; then
  echo "Failed to find purchase_price_average_14d"
  exit 1
fi

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

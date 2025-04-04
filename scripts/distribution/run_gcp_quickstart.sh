#!/bin/bash
set -xo pipefail


GREEN='\033[0;32m'
RED='\033[0;31m'

ENVIRONMENT="dev"

# Loop through all arguments
for arg in "$@"; do
  if [[ "$arg" == "--canary" ]]; then
    ENVIRONMENT="canary"
  fi
done

# Delete gcp tables to start from scratch
if [[ "$ENVIRONMENT" == "canary" ]]; then
  bq rm -f -t canary-443022:data.gcp_purchases_v1_test
  bq rm -f -t canary-443022:data.gcp_purchases_v1_test_upload
else
  bq rm -f -t canary-443022:data.gcp_purchases_v1_dev
  bq rm -f -t canary-443022:data.gcp_purchases_v1_dev_upload
fi
#TODO: delete bigtable rows

# Create a virtualenv to fresh install zipline-ai
VENV_DIR="tmp_chronon"
rm -rf $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Download the wheel
if [[ "$ENVIRONMENT" == "canary" ]]; then
  WHEEL_FILE=$(gcloud storage ls -l gs://zipline-artifacts-canary/release/candidate/wheels/ | grep zipline_ai | sort | tail -n 1 | awk '{print $3}' | xargs -n 1 basename )
  gcloud storage cp gs://zipline-artifacts-canary/release/candidate/wheels/${WHEEL_FILE} .
else
  WHEEL_FILE=$(gcloud storage ls -l gs://zipline-artifacts-dev/release/latest/wheels/ | grep zipline_ai | sort | tail -n 1 | awk '{print $3}' | xargs -n 1 basename )
  gcloud storage cp gs://zipline-artifacts-dev/release/latest/wheels/${WHEEL_FILE} .
fi
# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE


CHRONON_ROOT=`pwd`/api/python/test/canary
export PYTHONPATH="${PYTHONPATH}:$CHRONON_ROOT"


# function to check dataproc job id state
function check_dataproc_job_state() {
  JOB_ID=$1
  if [ -z "$JOB_ID" ]; then
        echo "No job id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  JOB_STATE=$(gcloud dataproc jobs describe $JOB_ID --region=us-central1 --format=flattened | grep "status.state:" | awk '{print $NF}')
  echo $JOB_STATE
#  TODO: this doesn't actually fail. need to fix.
  if [ -z "$JOB_STATE" ] || [ "$JOB_STATE" == "ERROR" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
}

DATAPROC_SUBMITTER_ID_STR="Dataproc submitter job id"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --chronon_root=$CHRONON_ROOT

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=canary zipline run --repo=$CHRONON_ROOT --mode backfill --conf compiled/group_bys/gcp/purchases.v1_test --dataproc --version candidate 2>&1 | tee tmp_backfill.out
else
  CUSTOMER_ID=dev zipline run --repo=$CHRONON_ROOT --mode backfill --conf compiled/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_backfill.out
fi
BACKFILL_JOB_ID=$(cat tmp_backfill.out | grep "$DATAPROC_SUBMITTER_ID_STR"  | cut -d " " -f5)
check_dataproc_job_state $BACKFILL_JOB_ID

echo -e "${GREEN}<<<<<.....................................GROUP-BY-UPLOAD.....................................>>>>>\033[0m"
touch tmp_gbu.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=canary zipline run --repo=$CHRONON_ROOT --mode upload --conf compiled/group_bys/gcp/purchases.v1_test --ds  2023-12-01 --dataproc --version candidate 2>&1 | tee tmp_gbu.out
else
  CUSTOMER_ID=dev zipline run --repo=$CHRONON_ROOT --mode upload --conf compiled/group_bys/gcp/purchases.v1_dev --ds  2023-12-01 --dataproc 2>&1 | tee tmp_gbu.out
fi
GBU_JOB_ID=$(cat tmp_gbu.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $GBU_JOB_ID

# Need to wait for upload to finish
echo -e "${GREEN}<<<<<.....................................UPLOAD-TO-KV.....................................>>>>>\033[0m"
touch tmp_upload_to_kv.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=canary zipline run --repo=$CHRONON_ROOT --mode upload-to-kv --conf compiled/group_bys/gcp/purchases.v1_test --partition-string=2023-12-01 --dataproc --version candidate 2>&1 | tee tmp_upload_to_kv.out
else
  CUSTOMER_ID=dev zipline run --repo=$CHRONON_ROOT --mode upload-to-kv --conf compiled/group_bys/gcp/purchases.v1_dev --partition-string=2023-12-01 --dataproc 2>&1 | tee tmp_upload_to_kv.out
fi
UPLOAD_TO_KV_JOB_ID=$(cat tmp_upload_to_kv.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $UPLOAD_TO_KV_JOB_ID

echo -e "${GREEN}<<<<< .....................................METADATA-UPLOAD.....................................>>>>>\033[0m"
touch tmp_metadata_upload.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=canary zipline run --repo=$CHRONON_ROOT --mode metadata-upload --conf compiled/group_bys/gcp/purchases.v1_test --dataproc --version candidate 2>&1 | tee tmp_metadata_upload.out
else
  CUSTOMER_ID=dev zipline run --repo=$CHRONON_ROOT --mode metadata-upload --conf compiled/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_metadata_upload.out
fi
METADATA_UPLOAD_JOB_ID=$(cat tmp_metadata_upload.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)
check_dataproc_job_state $METADATA_UPLOAD_JOB_ID

# Need to wait for upload-to-kv to finish
echo -e "${GREEN}<<<<<.....................................FETCH.....................................>>>>>\033[0m"
touch tmp_fetch.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=canary zipline run --repo=$CHRONON_ROOT --mode fetch --conf=compiled/group_bys/gcp/purchases.v1_test -k '{"user_id":"5"}' --name gcp.purchases.v1_test --version candidate  2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
else
  CUSTOMER_ID=dev zipline run --repo=$CHRONON_ROOT --mode fetch --conf=compiled/group_bys/gcp/purchases.v1_dev  -k '{"user_id":"5"}' --name gcp.purchases.v1_dev  2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
fi
cat tmp_fetch.out | grep purchase_price_average_14d
# check if exit code of previous is 0
if [ $? -ne 0 ]; then
  echo "Failed to find purchase_price_average_14d"
  exit 1
fi

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --canary | --dev          Specify the environment (canary or dev)"
    echo "  --version <version>       Specify the version you want to run"
    echo "  -h, --help  Show this help message"
}

if [ $# -ne 3 ]; then
    print_usage
    exit 1
fi

USE_DEV=false
USE_CANARY=false
VERSION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --canary)
            USE_CANARY=true
            shift
            ;;
        --dev)
            USE_DEV=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        --version)
            if [[ -z $2 ]]; then
                echo "Error: --version requires a value"
                print_usage
                exit 1
            fi
            VERSION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Set ENVIRONMENT based on flags
if [[ "$USE_CANARY" == true ]]; then
    ENVIRONMENT="canary"
elif [[ "$USE_DEV" == true ]]; then
    ENVIRONMENT="dev"
else
    echo "Error: You must specify either --canary or --dev."
    print_usage
    exit 1
fi

echo "Running with environment $ENVIRONMENT and version $VERSION"

set -xo pipefail

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
WHEEL_FILE="zipline_ai-$VERSION-py3-none-any.whl"
gcloud storage cp gs://zipline-artifacts-$ENVIRONMENT/release/$VERSION/wheels/$WHEEL_FILE .

# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE

CHRONON_ROOT=`pwd`/api/python/test/canary
export PYTHONPATH="$CHRONON_ROOT"


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

function fail_if_bash_failed() {
  if [ $? -ne 0 ]; then
    echo -e "${RED} <<<<<<<<<<<<<<<<-----------------FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
    exit 1
  fi
}

DATAPROC_SUBMITTER_ID_STR="Dataproc submitter job id"

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --chronon_root=$CHRONON_ROOT

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode backfill --conf compiled/group_bys/gcp/purchases.v1_test --dataproc 2>&1 | tee tmp_backfill.out
else
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_backfill.out
fi

fail_if_bash_failed $?

BACKFILL_JOB_ID=$(cat tmp_backfill.out | grep "$DATAPROC_SUBMITTER_ID_STR"  | cut -d " " -f5)

echo -e "${GREEN}<<<<<.....................................GROUP-BY-UPLOAD.....................................>>>>>\033[0m"
touch tmp_gbu.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload --conf compiled/group_bys/gcp/purchases.v1_test --ds  2023-12-01 --dataproc 2>&1 | tee tmp_gbu.out
else
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload --conf compiled/group_bys/gcp/purchases.v1_dev --ds  2023-12-01 --dataproc 2>&1 | tee tmp_gbu.out
fi
fail_if_bash_failed
GBU_JOB_ID=$(cat tmp_gbu.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)

# Need to wait for upload to finish
echo -e "${GREEN}<<<<<.....................................UPLOAD-TO-KV.....................................>>>>>\033[0m"
touch tmp_upload_to_kv.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload-to-kv --conf compiled/group_bys/gcp/purchases.v1_test --partition-string=2023-12-01 --dataproc 2>&1 | tee tmp_upload_to_kv.out
else
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload-to-kv --conf compiled/group_bys/gcp/purchases.v1_dev --partition-string=2023-12-01 --dataproc 2>&1 | tee tmp_upload_to_kv.out
fi
fail_if_bash_failed
UPLOAD_TO_KV_JOB_ID=$(cat tmp_upload_to_kv.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)

echo -e "${GREEN}<<<<< .....................................METADATA-UPLOAD.....................................>>>>>\033[0m"
touch tmp_metadata_upload.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode metadata-upload --conf compiled/group_bys/gcp/purchases.v1_test --dataproc 2>&1 | tee tmp_metadata_upload.out
else
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode metadata-upload --conf compiled/group_bys/gcp/purchases.v1_dev --dataproc 2>&1 | tee tmp_metadata_upload.out
fi
fail_if_bash_failed
METADATA_UPLOAD_JOB_ID=$(cat tmp_metadata_upload.out | grep "$DATAPROC_SUBMITTER_ID_STR" | cut -d " " -f5)

# Need to wait for upload-to-kv to finish
echo -e "${GREEN}<<<<<.....................................FETCH.....................................>>>>>\033[0m"
touch tmp_fetch.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode fetch --conf=compiled/group_bys/gcp/purchases.v1_test -k '{"user_id":"5"}' --name gcp.purchases.v1_test 2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
else
  CUSTOMER_ID=$ENVIRONMENT zipline run --repo=$CHRONON_ROOT --version $VERSION --mode fetch --conf=compiled/group_bys/gcp/purchases.v1_dev  -k '{"user_id":"5"}' --name gcp.purchases.v1_dev  2>&1 | tee tmp_fetch.out | grep -q purchase_price_average_14d
fi
fail_if_bash_failed
cat tmp_fetch.out | grep purchase_price_average_14d
# check if exit code of previous is 0
if [ $? -ne 0 ]; then
  echo "Failed to find purchase_price_average_14d"
  exit 1
fi

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

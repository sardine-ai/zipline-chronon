#!/bin/bash

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --all       Build and upload all artifacts (GCP and AWS)"
    echo "  --gcp       Build and upload only GCP artifacts"
    echo "  --aws       Build and upload only AWS artifacts"
    echo "  --aws_customer_ids <customer_id>  Specify AWS customer IDs to upload artifacts to."
    echo "  --gcp_customer_ids <customer_id>  Specify GCP customer IDs to upload artifacts to."
    echo "  -h, --help  Show this help message"
}

# No arguments provided
if [ $# -eq 0 ]; then
    print_usage
    exit 1
fi

BUILD_AWS=false
BUILD_GCP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            BUILD_GCP=true
            BUILD_AWS=true
            shift
            ;;
        --gcp)
            BUILD_GCP=true
            shift
            ;;
        --aws)
            BUILD_AWS=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        --aws_customer_ids)
            if [[ -z $2 ]]; then
                echo "Error: --customer_ids requires a value"
                print_usage
                exit 1
            fi
            INPUT_AWS_CUSTOMER_IDS=("$2")
            shift 2
            ;;
        --gcp_customer_ids)
            if [[ -z $2 ]]; then
                echo "Error: --customer_ids requires a value"
                print_usage
                exit 1
            fi
            INPUT_GCP_CUSTOMER_IDS=("$2")
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done


if [[ -n $(git diff HEAD) ]]; then
    echo "Error: You have uncommitted changes. Please commit and push them to git so we can track them."
    exit 1
fi

# Get current branch name
local_branch=$(git rev-parse --abbrev-ref HEAD)

# Fetch latest from remote
git fetch origin $local_branch

# Check if local is behind remote
if [[ -n $(git diff HEAD..origin/$local_branch) ]]; then
    echo "Error: Your branch is not in sync with remote"
    echo "Please push your local changes and sync your local branch $local_branch with remote"
    exit 1
fi

set -e

SCRIPT_DIRECTORY=$(dirname -- "$(realpath -- "$0")")
CHRONON_ROOT_DIR=$(dirname "$SCRIPT_DIRECTORY")

echo "Working in $CHRONON_ROOT_DIR"
cd $CHRONON_ROOT_DIR

echo "Building wheel"
#Check python version >= 3.9
MAJOR_PYTHON_VERSION=$(python --version | cut -d " " -f2 | cut -d "." -f 1)
MINOR_PYTHON_VERSION=$(python --version | cut -d " " -f2 | cut -d "." -f 2)

EXPECTED_MINIMUM_MAJOR_PYTHON_VERSION=3
EXPECTED_MINIMUM_MINOR_PYTHON_VERSION=9

if [[ $EXPECTED_MINIMUM_MAJOR_PYTHON_VERSION -gt $MAJOR_PYTHON_VERSION ]] ; then
    echo "Failed major version of $MAJOR_PYTHON_VERSION. Expecting python version of at least $EXPECTED_MINIMUM_MAJOR_PYTHON_VERSION.$EXPECTED_MINIMUM_MINOR_PYTHON_VERSION to build wheel. Your version is $(python --version)"
    exit 1
fi

if [[ $EXPECTED_MINIMUM_MINOR_PYTHON_VERSION -gt $MINOR_PYTHON_VERSION ]] ; then
    echo "Failed minor version of $MINOR_PYTHON_VERSION. Expecting python version of at least $EXPECTED_MINIMUM_MAJOR_PYTHON_VERSION.$EXPECTED_MINIMUM_MINOR_PYTHON_VERSION to build wheel. Your version is $(python --version)"
    exit 1
fi


thrift --gen py -out api/py/ api/thrift/common.thrift
thrift --gen py -out api/py/ api/thrift/api.thrift
thrift --gen py -out api/py/ api/thrift/observability.thrift
VERSION=$(cat version.sbt | cut -d " " -f3 | tr -d '"') pip wheel api/py
EXPECTED_ZIPLINE_WHEEL="zipline_ai-0.1.0.dev0-py3-none-any.whl"
if [ ! -f "$EXPECTED_ZIPLINE_WHEEL" ]; then
    echo "$EXPECTED_ZIPLINE_WHEEL not found"
    exit 1
fi

echo "Building jars"

bazel build //flink:flink_assembly_deploy.jar
bazel build //service:service_assembly_deploy.jar

FLINK_JAR="$CHRONON_ROOT_DIR/bazel-bin/flink/flink_assembly_deploy.jar"
SERVICE_JAR="$CHRONON_ROOT_DIR/bazel-bin/service/service_assembly_deploy.jar"

if [ ! -f "$SERVICE_JAR" ]; then
    echo "$SERVICE_JAR not found"
    exit 1
fi

if [ ! -f "$FLINK_JAR" ]; then
    echo "$FLINK_JAR not found"
    exit 1
fi



if [ "$BUILD_AWS" = true ]; then
    bazel build //cloud_aws:cloud_aws_lib_deploy.jar

    CLOUD_AWS_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_aws/cloud_aws_lib_deploy.jar"

    if [ ! -f "$CLOUD_AWS_JAR" ]; then
        echo "$CLOUD_AWS_JAR not found"
        exit 1
    fi
fi
if [ "$BUILD_GCP" = true ]; then
    bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar
    bazel build //cloud_gcp:cloud_gcp_submitter_deploy.jar

    CLOUD_GCP_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_gcp/cloud_gcp_lib_deploy.jar"
    CLOUD_GCP_SUBMITTER_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_gcp/cloud_gcp_submitter_deploy.jar"

    if [ ! -f "$CLOUD_GCP_JAR" ]; then
        echo "$CLOUD_GCP_JAR not found"
        exit 1
    fi

    if [ ! -f "$CLOUD_GCP_SUBMITTER_JAR" ]; then
        echo "$CLOUD_GCP_SUBMITTER_JAR not found"
        exit 1
    fi

fi




# all customer ids
GCP_CUSTOMER_IDS=("canary" "etsy")

# Takes in array of customer ids
function upload_to_gcp() {
  # Disabling this so that we can set the custom metadata on these jars
  gcloud config set storage/parallel_composite_upload_enabled False
  customer_ids_to_upload=("$@")
  echo "Are you sure you want to upload to these customer ids: ${customer_ids_to_upload[*]}"
  select yn in "Yes" "No"; do
      case $yn in
          Yes )
              set -euxo pipefail
              for element in "${customer_ids_to_upload[@]}"
              do
                ELEMENT_JAR_PATH=gs://zipline-artifacts-$element/jars
                gcloud storage cp "$CLOUD_GCP_JAR" "$ELEMENT_JAR_PATH" --custom-metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                gcloud storage cp "$CLOUD_GCP_SUBMITTER_JAR" "$ELEMENT_JAR_PATH" --custom-metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                gcloud storage cp "$SERVICE_JAR" "$ELEMENT_JAR_PATH" --custom-metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                gcloud storage cp "$EXPECTED_ZIPLINE_WHEEL" "$ELEMENT_JAR_PATH" --custom-metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                gcloud storage cp "$FLINK_JAR" "$ELEMENT_JAR_PATH" --custom-metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
              done
              echo "Succeeded"
              break;;
          No ) break;;
      esac
  done
  gcloud config set storage/parallel_composite_upload_enabled True
}

AWS_CUSTOMER_IDS=("canary")

# Takes in array of customer ids
function upload_to_aws() {
  customer_ids_to_upload=("$@")
  echo "Are you sure you want to upload to these customer ids: ${customer_ids_to_upload[*]}"
  select yn in "Yes" "No"; do
      case $yn in
          Yes )
              set -euxo pipefail
              for element in "${customer_ids_to_upload[@]}"
              do
                ELEMENT_JAR_PATH=s3://zipline-artifacts-$element/jars/
                aws s3 cp "$CLOUD_AWS_JAR" "$ELEMENT_JAR_PATH" --metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                aws s3 cp "$SERVICE_JAR" "$ELEMENT_JAR_PATH" --metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                aws s3 cp "$EXPECTED_ZIPLINE_WHEEL" "$ELEMENT_JAR_PATH" --metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
                aws s3 cp "$FLINK_JAR" "$ELEMENT_JAR_PATH" --metadata="zipline_user=$USER,updated_date=$(date),commit=$(git rev-parse HEAD),branch=$(git rev-parse --abbrev-ref HEAD)"
              done
              echo "Succeeded"
              break;;
          No ) break;;
      esac
  done
}


if [ "$BUILD_AWS" = true ]; then
  if [  ${#INPUT_AWS_CUSTOMER_IDS[@]} -eq 0 ]; then
      echo "No customer ids provided for AWS. Using default: ${AWS_CUSTOMER_IDS[*]}"
    else
      AWS_CUSTOMER_IDS=("${INPUT_AWS_CUSTOMER_IDS[@]}")
    fi
  upload_to_aws "${AWS_CUSTOMER_IDS[@]}"
fi
if [ "$BUILD_GCP" = true ]; then
  if [  ${#INPUT_GCP_CUSTOMER_IDS[@]} -eq 0 ]; then
    echo "No customer ids provided for GCP. Using default: ${GCP_CUSTOMER_IDS[*]}"
  else
    GCP_CUSTOMER_IDS=("${INPUT_GCP_CUSTOMER_IDS[@]}")
  fi
  upload_to_gcp "${GCP_CUSTOMER_IDS[@]}"
fi


# Cleanup wheel stuff
rm ./*.whl
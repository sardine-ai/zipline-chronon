#!/bin/bash

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

if [[ EXPECTED_MINIMUM_MINOR_PYTHON_VERSION -gt MINOR_PYTHON_VERSION ]] ; then
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
sbt clean
sbt service/assembly

bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar
bazel build //cloud_gcp:cloud_gcp_submitter_deploy.jar
bazel build //flink:flink_assembly_deploy.jar

CLOUD_GCP_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_gcp/cloud_gcp_lib_deploy.jar"
CLOUD_GCP_SUBMITTER_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_gcp/cloud_gcp_submitter_deploy.jar"
FLINK_JAR="$CHRONON_ROOT_DIR/bazel-bin/flink/flink_assembly_deploy.jar"
SERVICE_JAR="$CHRONON_ROOT_DIR/service/target/scala-2.12/service-0.1.0-SNAPSHOT.jar"

if [ ! -f "$CLOUD_GCP_JAR" ]; then
    echo "$CLOUD_GCP_JAR not found"
    exit 1
fi

if [ ! -f "$CLOUD_GCP_SUBMITTER_JAR" ]; then
    echo "$CLOUD_GCP_SUBMITTER_JAR not found"
    exit 1
fi

if [ ! -f "$SERVICE_JAR" ]; then
    echo "$SERVICE_JAR not found"
    exit 1
fi

if [ ! -f "$FLINK_JAR" ]; then
    echo "$FLINK_JAR not found"
    exit 1
fi

# all customer ids
ALL_CUSTOMER_IDS=("canary" "our clients")

# Takes in array of customer ids
function upload_to_gcp() {
  customer_ids_to_upload=("$@")
  echo "Are you sure you want to upload to these customer ids: ${customer_ids_to_upload[*]}"
  select yn in "Yes" "No"; do
      case $yn in
          Yes )
              set -euxo pipefail
              for element in "${customer_ids_to_upload[@]}"
              do
                ELEMENT_JAR_PATH=gs://zipline-artifacts-$element/jars
                gcloud storage cp "$CLOUD_GCP_JAR" "$ELEMENT_JAR_PATH";
                gcloud storage cp "$CLOUD_GCP_SUBMITTER_JAR" "$ELEMENT_JAR_PATH";
                gcloud storage cp "$SERVICE_JAR" "$ELEMENT_JAR_PATH"
                gcloud storage cp "$EXPECTED_ZIPLINE_WHEEL" "$ELEMENT_JAR_PATH"
                gcloud storage cp "$FLINK_JAR" "$ELEMENT_JAR_PATH"
              done
              echo "Succeeded"
              break;;
          No ) break;;
      esac
  done
}

# check if $1 (single customer id mode) has been set
if [ -z "$1" ]; then
  upload_to_gcp "${ALL_CUSTOMER_IDS[@]}"
else
  upload_to_gcp "$1"
fi

# Cleanup wheel stuff
rm ./*.whl
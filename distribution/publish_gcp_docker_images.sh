#!/bin/bash


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

echo "Building jars"

bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar
bazel build //service:service_assembly_deploy.jar

CLOUD_GCP_JAR="$CHRONON_ROOT_DIR/bazel-bin/cloud_gcp/cloud_gcp_lib_deploy.jar"
SERVICE_JAR="$CHRONON_ROOT_DIR/bazel-bin/service/service_assembly_deploy.jar"

if [ ! -f "$CLOUD_GCP_JAR" ]; then
    echo "$CLOUD_GCP_JAR not found"
    exit 1
fi

if [ ! -f "$SERVICE_JAR" ]; then
    echo "$SERVICE_JAR not found"
    exit 1
fi

# We copy to build output as the docker build can't access the bazel-bin (as its a symlink)
echo "Copying jars to build_output"
mkdir -p build_output
cp bazel-bin/service/service_assembly_deploy.jar build_output/
cp bazel-bin/cloud_aws/cloud_aws_lib_deploy.jar build_output/
cp bazel-bin/cloud_gcp/cloud_gcp_lib_deploy.jar build_output/

echo "Kicking off a docker login"
docker login

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/fetcher/Dockerfile \
  -t ziplineai/chronon-fetcher:$(git rev-parse --short HEAD) \
  -t ziplineai/chronon-fetcher:latest \
  --push \
  .

# Clean up build output dir
rm -rf build_output

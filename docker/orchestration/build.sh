#!/bin/bash
set -e

bazel build //orchestration:orchestration_assembly_deploy.jar

rm -rf ./build-outputs || true

mkdir ./build-outputs || true

cp bazel-bin/orchestration/orchestration_assembly_deploy.jar ./build-outputs/

cp ~/.config/gcloud/application_default_credentials.json ./build-outputs/application_default_credentials.json

docker build -t orchestration:latest -f docker/orchestration/Dockerfile . \
--build-arg GCP_REGION=us-central1 \
--build-arg GCP_PROJECT_ID=zipline-main \
--build-arg CUSTOMER_ID=canary
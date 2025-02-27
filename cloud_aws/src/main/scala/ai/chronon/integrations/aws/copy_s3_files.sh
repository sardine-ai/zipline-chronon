#!/bin/bash
set -euxo pipefail
pwd

# Loop through all provided arguments (files). Copies files from S3 to /mnt/zipline/
for s3_file in "$@"; do
    aws s3 cp $s3_file /mnt/zipline/
done
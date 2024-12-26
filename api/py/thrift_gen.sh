#!/bin/bash

echo "Testing thrift py gen + compile"
echo "python path is set to: $PYTHONPATH"

echo "Cleaning up existing thrift files"
rm -rf ./ai/chronon/api
rm -rf ./ai/chronon/observability

echo "Generating thrift files"
thrift --gen py -out . ../thrift/common.thrift
thrift --gen py -out . ../thrift/api.thrift
thrift --gen py -out . ../thrift/observability.thrift
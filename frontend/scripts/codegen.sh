#!/bin/bash

OUT_DIR="frontend/src/lib/types/codegen"
THRIFT_DIR="api/thrift"

# Clear the output directory
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# Find all .thrift files and join them with spaces, but strip the path prefix
THRIFT_FILES=$(find "../$THRIFT_DIR" -name "*.thrift" -exec basename {} \; | tr '\n' ' ')

# Using npx to run thrift-typescript with absolute paths
npx thrift-typescript \
    --rootDir ".." \
    --outDir "$OUT_DIR" \
    --sourceDir "$THRIFT_DIR" \
    --target thrift-server \
    --fallbackNamespace none \
    $THRIFT_FILES

#!/bin/bash

THRIFT_DIR="../api/thrift"
OUT_DIR="src/lib/types/codegen"

mkdir -p $OUT_DIR

for file in $THRIFT_DIR/*.thrift; do
    thrift -r -out $OUT_DIR --gen js:ts "$file"
done 
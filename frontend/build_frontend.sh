#!/bin/bash
set -e

# Install dependencies
npm ci

# Build the project
npm run build

# Copy to public directory
mkdir -p ../hub/public
rm -rf ../hub/public/*
cp -r build/* ../hub/public/ 
#!/bin/bash

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --all       Build all modules (Spark, Frontend, Hub)"
    echo "  --clean     Clean and build all modules"
    echo "  --spark     Build Spark modules only"
    echo "  --frontend  Build frontend modules only"
    echo "  --hub       Build Hub modules only"
    echo "  -h, --help  Show this help message"
}

# No arguments provided
if [ $# -eq 0 ]; then
    print_usage
    exit 1
fi

# Process command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            BUILD_ALL=true
            shift
            ;;
        --clean)
            CLEAN=true
            BUILD_ALL=true
            shift
            ;;
        --spark)
            BUILD_SPARK=true
            shift
            ;;
        --frontend)
            BUILD_FRONTEND=true
            shift
            ;;
        --hub)
            BUILD_HUB=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Clean if requested
if [ "$CLEAN" = true ]; then
    echo "Cleaning project..."
    sbt clean
fi

# Build all modules
if [ "$BUILD_ALL" = true ]; then
    echo "Building all modules..."
    sbt assembly
    sbt dist
    sbt "project frontend" buildFrontend
fi

# Build Spark modules
if [ "$BUILD_SPARK" = true ]; then
    echo "Building Spark modules..."
    sbt assembly
fi

# Build Hub modules
if [ "$BUILD_HUB" = true ]; then
    echo "Building Hub distribution..."
    sbt dist
fi

# Build frontend
if [ "$BUILD_FRONTEND" = true ]; then
  echo "Building frontend distribution..."
  sbt "project frontend" buildFrontend
fi

echo "Wrapped up Chronon build. Triggering docker compose..."
docker compose -f docker-init/compose.yaml up --build

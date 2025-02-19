#!/bin/bash

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --all       Build all modules (Spark, Frontend, Hub, Cloud)"
    echo "  --clean     Clean and build all modules"
    echo "  --spark     Build Spark modules only"
    echo "  --frontend  Build frontend modules only"
    echo "  --hub       Build hub modules only"
    echo "  --cloud     Build cloud modules only"
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
        --cloud)
            BUILD_CLOUD=true
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
    bazel clean
fi

# Function to copy JAR files to build artifacts
function copy_jar_files() {
    # Get the bazel-bin directory
    bazel_bin=$(bazel info bazel-bin)
    
    # Create a directory for the build artifacts if it doesn't exist
    mkdir -p docker-init/build-artifacts
    
    # Copy the specified JAR file, using -f to force overwrite if file exists
    case $1 in
        "spark")
            cp -f "${bazel_bin}/spark/spark_assembly_deploy.jar" docker-init/build-artifacts/
            ;;
        "hub")
            cp -f "${bazel_bin}/hub/hub_assembly_deploy.jar" docker-init/build-artifacts/
            ;;
        "cloud_aws")
            cp -f "${bazel_bin}/cloud_aws/cloud_aws_lib_deploy.jar" docker-init/build-artifacts/
            ;;
    esac
}

# Build all modules
if [ "$BUILD_ALL" = true ]; then
    echo "Building all modules..."
    bazel build \
        //spark:spark_assembly_deploy.jar \
        //hub:hub_assembly_deploy.jar \
        //cloud_aws:cloud_aws_lib_deploy.jar

    copy_jar_files "spark"
    copy_jar_files "hub"
    copy_jar_files "cloud_aws"

    (cd frontend && ./build_frontend.sh)

fi

# Build Spark modules
if [ "$BUILD_SPARK" = true ]; then
    echo "Building Spark modules..."
    bazel build //spark:spark_assembly_deploy.jar
    copy_jar_files "spark"
fi

# Build frontend
if [ "$BUILD_FRONTEND" = true ]; then
    echo "Building frontend distribution..."
    (cd frontend && ./build_frontend.sh)
fi

# Build hub
if [ "$BUILD_HUB" = true ]; then
    echo "Building hub..."
    bazel build //hub:hub_assembly_deploy.jar
    copy_jar_files "hub"
fi

# Build cloud modules
if [ "$BUILD_CLOUD" = true ]; then
    echo "Building cloud modules..."
    bazel build //cloud_aws:cloud_aws_lib_deploy.jar
    copy_jar_files "cloud_aws"
fi

echo "Wrapped up Chronon build. Triggering docker compose..."
docker compose -f docker-init/compose.yaml up --build

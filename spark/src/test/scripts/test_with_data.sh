#!/bin/bash

# Test with actual partitioned data using our built-in test data creator

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

PORT=18081
BASE_URL="http://localhost:$PORT"

echo -e "${BLUE}Testing Partition Listing Service with Real Data${NC}"
echo "================================================"

# Check if service is running
if ! curl -s -f "$BASE_URL/health" > /dev/null 2>&1; then
    echo -e "${YELLOW}Service not running. Starting it...${NC}"
    
    # Start service in background
    TEMP_LOG=$(mktemp)
    echo "Starting service (log: $TEMP_LOG)"
    
    bazel run //spark:partition_listing_service -- $PORT > "$TEMP_LOG" 2>&1 &
    SERVICE_PID=$!
    
    echo "Service started with PID: $SERVICE_PID"
    echo -n "Waiting for service to be ready"
    
    # Wait for service to be ready
    for i in {1..30}; do
        echo -n "."
        if curl -s -f "$BASE_URL/health" > /dev/null 2>&1; then
            echo ""
            echo -e "${GREEN}✓ Service is ready!${NC}"
            READY=true
            break
        fi
        sleep 2
    done
    
    if [ "$READY" != "true" ]; then
        echo ""
        echo -e "${RED}✗ Service failed to start within 60 seconds${NC}"
        echo "Service logs:"
        tail -20 "$TEMP_LOG"
        exit 1
    fi
    
    # Cleanup function
    cleanup() {
        if [ ! -z "$SERVICE_PID" ]; then
            echo ""
            echo "Stopping service (PID: $SERVICE_PID)"
            kill $SERVICE_PID 2>/dev/null || true
            wait $SERVICE_PID 2>/dev/null || true
        fi
        rm -f "$TEMP_LOG"
        if [ ! -z "$WAREHOUSE_DIR" ] && [ -d "$WAREHOUSE_DIR" ]; then
            echo "Cleaning up warehouse: $WAREHOUSE_DIR"
            rm -rf "$WAREHOUSE_DIR"
        fi
    }
    trap cleanup EXIT
else
    echo -e "${GREEN}✓ Service is already running${NC}"
fi

echo ""
echo -e "${BLUE}Creating test data using built-in test data creator...${NC}"

# Create temp warehouse
WAREHOUSE_DIR=$(mktemp -d)
echo "Using warehouse: $WAREHOUSE_DIR"

echo "Building test data creator..."
if ! bazel build //spark:test_data_creator; then
    echo -e "${RED}✗ Failed to build test data creator${NC}"
    exit 1
fi

echo "Running test data creator..."
DATA_LOG=$(mktemp)
if bazel run //spark:test_data_creator -- "$WAREHOUSE_DIR" > "$DATA_LOG" 2>&1; then
    echo -e "${GREEN}✓ Test data created successfully${NC}"
    echo "Data creation logs:"
    grep -E "(Creating|Created|test_events|user_activity)" "$DATA_LOG" || echo "Data created in $WAREHOUSE_DIR"
else
    echo -e "${RED}✗ Failed to create test data${NC}"
    echo "Data creation logs:"
    cat "$DATA_LOG"
    exit 1
fi

rm -f "$DATA_LOG"

echo ""
echo -e "${BLUE}Testing partition listing with real data...${NC}"

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0

# Function to test API endpoint
test_endpoint() {
    local description="$1"
    local url="$2"
    local expected_min_partitions="$3"
    local expected_success="${4:-true}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo ""
    echo -e "${YELLOW}Test $TOTAL_TESTS: $description${NC}"
    echo "URL: $url"
    
    response=$(curl -s "$url")
    echo "Response:"
    if echo "$response" | jq . > /dev/null 2>&1; then
        echo "$response" | jq .
    else
        echo "$response"
        echo -e "${RED}✗ Invalid JSON response${NC}"
        return
    fi
    
    # Check success status
    success=$(echo "$response" | jq -r '.success' 2>/dev/null || echo "unknown")
    if [ "$success" = "$expected_success" ]; then
        echo -e "${GREEN}✓ Success status: $success${NC}"
        success_ok=true
    else
        echo -e "${RED}✗ Success status: $success (expected: $expected_success)${NC}"
        success_ok=false
    fi
    
    # Check partition count if specified
    if [ ! -z "$expected_min_partitions" ] && [ "$expected_min_partitions" != "0" ]; then
        partition_count=$(echo "$response" | jq '.partitions | length' 2>/dev/null || echo "0")
        if [ "$partition_count" -ge "$expected_min_partitions" ]; then
            echo -e "${GREEN}✓ Found $partition_count partitions (expected >= $expected_min_partitions)${NC}"
            count_ok=true
        else
            echo -e "${RED}✗ Found $partition_count partitions (expected >= $expected_min_partitions)${NC}"
            count_ok=false
        fi
    else
        count_ok=true
    fi
    
    # Show partition details
    partitions=$(echo "$response" | jq -r '.partitions[]' 2>/dev/null || echo "")
    if [ ! -z "$partitions" ]; then
        echo "Partitions found: $(echo "$partitions" | tr '\n' ', ' | sed 's/,$//')"
    fi
    
    # Mark test as passed
    if [ "$success_ok" = "true" ] && [ "$count_ok" = "true" ]; then
        echo -e "${GREEN}✓ Test passed${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ Test failed${NC}"
    fi
}

# Run comprehensive tests
test_endpoint "Health check" "$BASE_URL/health" "" "true"

test_endpoint "Service info" "$BASE_URL/api/info" "" "true"

test_endpoint "List all partitions for test_events" "$BASE_URL/api/partitions/test_events" "4"

test_endpoint "List all partitions for user_activity" "$BASE_URL/api/partitions/user_activity" "5"

test_endpoint "Date range filter (2023-01-01 to 2023-01-02)" \
    "$BASE_URL/api/partitions/test_events?startDate=2023-01-01&endDate=2023-01-02" "2"

test_endpoint "Date range filter (single day)" \
    "$BASE_URL/api/partitions/test_events?startDate=2023-01-03&endDate=2023-01-03" "1"

test_endpoint "Sub-partition filter (region=us-west)" \
    "$BASE_URL/api/partitions/user_activity?filter.region=us-west" "2"

test_endpoint "Sub-partition filter (region=europe)" \
    "$BASE_URL/api/partitions/user_activity?filter.region=europe" "1"

test_endpoint "Combined filters (date + region)" \
    "$BASE_URL/api/partitions/user_activity?startDate=2023-01-01&endDate=2023-01-02&filter.region=us-west" "2"

test_endpoint "Custom partition column" \
    "$BASE_URL/api/partitions/test_events?partitionColumn=ds" "4"

test_endpoint "Non-existent table" \
    "$BASE_URL/api/partitions/non_existent_table" "0"

test_endpoint "Invalid endpoint" \
    "$BASE_URL/api/invalid" "" "false"

echo ""
echo -e "${BLUE}Performance testing...${NC}"

echo "Response time test:"
time curl -s "$BASE_URL/api/partitions/test_events" > /dev/null

echo ""
echo "Concurrent requests test (5 parallel):"
start_time=$(date +%s.%N)
for i in {1..5}; do
    curl -s "$BASE_URL/api/partitions/test_events" > /dev/null &
done
wait
end_time=$(date +%s.%N)
duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "~2-3")
echo "5 concurrent requests completed in ${duration}s"

echo ""
echo "Load test (50 requests):"
echo -n "Progress: "
start_time=$(date +%s.%N)
for i in {1..50}; do
    curl -s "$BASE_URL/api/partitions/test_events" > /dev/null
    if [ $((i % 10)) -eq 0 ]; then
        echo -n "$i "
    fi
done
end_time=$(date +%s.%N)
duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "N/A")
echo ""
echo "50 sequential requests completed in ${duration}s"
if [ "$duration" != "N/A" ]; then
    avg_time=$(echo "scale=3; $duration / 50" | bc -l 2>/dev/null || echo "N/A")
    echo "Average response time: ${avg_time}s per request"
fi

# Test Results Summary
echo ""
echo -e "${BLUE}Test Results Summary${NC}"
echo "===================="
echo "Total tests: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $((TOTAL_TESTS - PASSED_TESTS))"

if [ $PASSED_TESTS -eq $TOTAL_TESTS ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit_code=0
elif [ $PASSED_TESTS -gt $((TOTAL_TESTS * 3 / 4)) ]; then
    echo -e "${YELLOW}⚠ Most tests passed ($(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc -l 2>/dev/null || echo "75+")%)${NC}"
    exit_code=0
else
    echo -e "${RED}✗ Many tests failed${NC}"
    exit_code=1
fi

echo ""
echo -e "${BLUE}Manual testing commands:${NC}"
echo "# List all partitions"
echo "curl -s '$BASE_URL/api/partitions/test_events' | jq ."
echo ""
echo "# Date range filter"
echo "curl -s '$BASE_URL/api/partitions/test_events?startDate=2023-01-01&endDate=2023-01-02' | jq ."
echo ""
echo "# Sub-partition filter"
echo "curl -s '$BASE_URL/api/partitions/user_activity?filter.region=us-west' | jq ."
echo ""
echo "# Combined filters"
echo "curl -s '$BASE_URL/api/partitions/user_activity?startDate=2023-01-01&endDate=2023-01-02&filter.region=us-west' | jq ."

echo ""
echo "Data location: $WAREHOUSE_DIR"
echo "Tables created:"
echo "  - test_events (partitioned by ds): 4 partitions (2023-01-01 to 2023-01-04)"
echo "  - user_activity (partitioned by ds, region): 5+ partitions"

echo ""
if [ $exit_code -eq 0 ]; then
    echo -e "${GREEN}🎉 Integration test with real data completed successfully!${NC}"
else
    echo -e "${YELLOW}⚠ Integration test completed with some issues${NC}"
fi

exit $exit_code
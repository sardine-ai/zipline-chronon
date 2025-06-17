# Partition Listing Service Test Scripts

This directory contains test scripts for the Chronon Partition Listing Service.

## Scripts Overview

### 1. `test_partition_service.sh` - Comprehensive Integration Test
**Purpose**: Full automated integration test suite  
**Features**:
- Starts the partition service automatically
- Creates comprehensive test data using Spark
- Runs extensive curl tests
- Includes error handling and cleanup

**Usage**:
```bash
# Run with default settings
./test_partition_service.sh

# Run with custom port
TEST_PORT=9090 ./test_partition_service.sh
```

**What it tests**:
- Service health and info endpoints
- Basic partition listing
- Date range filtering
- Sub-partition filtering
- Custom partition column names
- Error cases (non-existent tables, invalid endpoints)
- Concurrent request handling

### 2. `manual_curl_tests.sh` - Manual Test Guide
**Purpose**: Interactive guide for manual testing  
**Features**:
- Step-by-step instructions
- Example curl commands
- Sample data creation scripts
- Performance testing examples

**Usage**:
```bash
# View the test guide
./manual_curl_tests.sh

# Follow the instructions to create test data and run curl commands
```

### 3. `quick_test.sh` - Fast Verification Test
**Purpose**: Quick smoke test for basic functionality  
**Features**:
- Creates minimal test data
- Runs essential curl tests
- Fast execution (~30 seconds)
- Good for CI/CD pipelines

**Usage**:
```bash
# Make sure service is running first
bazel run //spark:partition_listing_service &

# Then run quick test
./quick_test.sh
```

## Prerequisites

All scripts require:
- `curl` - for HTTP requests
- `jq` - for JSON formatting (optional but recommended)
- `bazel` - for building and running the service
- `spark-sql` or `spark-shell` - for creating test data

## Test Data

The scripts create various types of partitioned tables:

### Simple Date Partitioned
```sql
CREATE TABLE test_events (
  id BIGINT,
  user_id BIGINT,
  event_type STRING
) PARTITIONED BY (ds STRING);
```

### Multi-level Partitioned
```sql
CREATE TABLE user_activity (
  user_id BIGINT,
  activity STRING
) PARTITIONED BY (ds STRING, region STRING);
```

### Hourly Partitioned
```sql
CREATE TABLE transactions (
  transaction_id STRING,
  amount DECIMAL(10,2)
) PARTITIONED BY (date_hour STRING);
```

## API Endpoints Tested

### Health & Info
- `GET /health` - Service health check
- `GET /api/info` - Service information

### Partition Listing
- `GET /api/partitions/{tableName}` - Basic partition listing
- `GET /api/partitions/{tableName}?startDate=X&endDate=Y` - Date range filtering
- `GET /api/partitions/{tableName}?filter.{column}={value}` - Sub-partition filtering
- `GET /api/partitions/{tableName}?partitionColumn={name}` - Custom partition column

## Expected Responses

### Successful Response
```json
{
  "tableName": "test_events",
  "partitions": ["2023-01-01", "2023-01-02", "2023-01-03"],
  "success": true,
  "message": null
}
```

### Error Response
```json
{
  "tableName": "non_existent_table",
  "partitions": [],
  "success": false,
  "message": "Error: Table not found"
}
```

## Running Tests

### Option 1: Automated Test Suite
```bash
# Run comprehensive tests (recommended)
./test_partition_service.sh
```

### Option 2: Manual Testing
```bash
# Start service
bazel run //spark:partition_listing_service -- 8080 &

# Create test data manually
spark-sql --conf spark.sql.warehouse.dir=/tmp/warehouse
# ... create tables as shown in manual_curl_tests.sh

# Run curl commands
curl -s http://localhost:8080/api/partitions/your_table | jq .
```

### Option 3: Quick Verification
```bash
# Start service
bazel run //spark:partition_listing_service -- 8080 &

# Run quick test
./quick_test.sh
```

## Integration with CI/CD

For continuous integration, use the automated test:

```yaml
# Example GitHub Actions step
- name: Test Partition Service
  run: |
    cd spark/src/test/scripts
    ./test_partition_service.sh
```

## Troubleshooting

### Service Won't Start
- Check if port is already in use: `lsof -i :8080`
- Check service logs in temp directory
- Verify Bazel build succeeded: `bazel build //spark:partition_listing_service`

### Test Data Creation Fails
- Ensure Spark is properly installed
- Check warehouse directory permissions
- Verify Spark configuration

### Curl Tests Fail
- Confirm service is running: `curl -s http://localhost:8080/health`
- Check network connectivity
- Verify table names match created tables

### JSON Parsing Issues
- Install `jq`: `sudo apt-get install jq` (Ubuntu) or `brew install jq` (macOS)
- Or view raw responses without `jq`

## Performance Testing

The scripts include basic performance tests:

```bash
# Concurrent requests
for i in {1..10}; do
  curl -s http://localhost:8080/api/partitions/test_table &
done
wait

# Response time measurement
time curl -s http://localhost:8080/api/partitions/test_table > /dev/null
```

## Extending Tests

To add new test cases:

1. **Add test data** in the data generation section
2. **Add curl test** in the test execution section  
3. **Update expected results** in assertions
4. **Document new features** in this README

Example new test:
```bash
# Test new filtering feature
test_curl "Custom Filter" "$BASE_URL/api/partitions/table?filter.status=active"
```
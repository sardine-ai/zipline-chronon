# Partition Listing Service Tests

Test scripts for the Chronon Partition Listing Service.

## Running Tests

### Integration Test (Scala)
```bash
bazel test //spark:partitions_test
```

### Manual API Testing
```bash
# Start service
bazel run //spark:partition_listing_service -- 8080 &

# Test endpoints
curl http://localhost:8080/health
curl http://localhost:8080/api/partitions/my_table
```

## Test Coverage

The integration test validates:
- Health check and service info endpoints
- Basic partition listing
- Date range filtering (`startDate`, `endDate`)
- Sub-partition filtering (`filter.column=value`)
- Custom partition column names
- Error handling (non-existent tables, invalid endpoints)

## Manual Testing

For manual validation:
1. Start the service with custom port
2. Create test tables with partitions
3. Use curl to test various endpoints and parameters
4. Verify JSON responses match expected format

## Troubleshooting

- **Port conflicts**: Use different port with `-- 9090`
- **Network binding**: Check firewall and localhost access
- **Table access**: Verify Spark can access test tables
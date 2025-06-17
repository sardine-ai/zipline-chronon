# Chronon Partition Listing Service

HTTP REST API service for listing table partitions across Hive, Iceberg, and Delta Lake tables.

## Features
- Multi-format table support with automatic detection
- Date range and sub-partition filtering
- RESTful API with JSON responses

## Architecture
- **Service Layer**: HTTP endpoints using Netty
- **Catalog Layer**: TableUtils for format-agnostic partition listing
- **Format Layer**: Format-specific implementations via FormatProvider

## Usage

### Start Service
```bash
bazel run //spark:partition_listing_service -- 8080
```

### Programmatic Usage
```scala
val service = new PartitionListingService(spark, port = 8080)
service.start()
```

## API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```

### List Partitions
```bash
# Basic listing
curl http://localhost:8080/api/partitions/my_table

# With date range
curl "http://localhost:8080/api/partitions/my_table?startDate=2023-01-01&endDate=2023-01-31"

# With filters
curl "http://localhost:8080/api/partitions/my_table?filter.region=us-west"

# Custom partition column
curl "http://localhost:8080/api/partitions/my_table?partitionColumn=date"

# Custom partition format
curl "http://localhost:8080/api/partitions/my_table?partitionFormat=yyyyMMdd"

# Hourly partitions
curl "http://localhost:8080/api/partitions/my_table?partitionInterval=hourly&partitionFormat=yyyy-MM-dd-HH"

# Custom partition spec (all parameters)
curl "http://localhost:8080/api/partitions/my_table?partitionColumn=event_date&partitionFormat=yyyy-MM-dd&partitionInterval=daily"
```

## Response Format
```json
{
  "tableName": "my_table",
  "partitions": ["2023-01-01", "2023-01-02", "2023-01-03"],
  "success": true,
  "message": null
}
```

## Query Parameters
- `startDate`, `endDate` - Date range filtering
- `filter.{column}={value}` - Sub-partition filtering  
- `partitionColumn` - Custom partition column name (default: "ds")
- `partitionFormat` - Date format pattern (default: "yyyy-MM-dd")
- `partitionInterval` - Partition interval: "hourly", "daily", or milliseconds (default: "daily")

## Integration
Uses existing Chronon components:
- `TableUtils` for partition listing
- `FormatProvider` for table format detection
- Existing partition filtering logic
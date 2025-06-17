# Chronon Partition Listing Service

This module provides an HTTP REST API service that exposes table partition listing functionality for Apache Spark tables.

## Features

- **Multi-format support**: Works with Hive, Iceberg, and Delta Lake tables
- **Automatic format detection**: Automatically detects table format and uses appropriate partition listing strategy  
- **Filtering capabilities**: Supports partition range filtering and sub-partition filtering
- **RESTful API**: HTTP endpoints for easy integration
- **Spark integration**: Built on Apache Spark for robust table format support

## Components

### PartitionListingService
Complete HTTP service that provides REST endpoints for partition listing:
- `GET /health` - Health check endpoint
- `GET /api/partitions/{tableName}` - List partitions for a table
- `GET /api/info` - Service information
- Manages SparkSession lifecycle
- Built-in Netty HTTP server
- Command-line argument support

## Usage

### Starting the Server

```bash
# Using Bazel (recommended)
bazel run //spark:partition_listing_service -- 8080

# Custom port
bazel run //spark:partition_listing_service -- 9090
```

### Programmatic Usage
```scala
val spark = SparkSession.builder()
  .appName("My App")
  .master("local[*]")
  .getOrCreate()

val service = new PartitionListingService(spark, port = 8080)
service.start() // This will block
```

### Environment Variables
- `PARTITION_HOST` - Host to bind to (default: localhost)  
- `PARTITION_PORT` - Port for HTTP API (default: 8080)

## API Examples

### Basic partition listing
```bash
curl http://localhost:15003/api/partitions/my_database.my_table
```

### With partition column filter
```bash
curl "http://localhost:15003/api/partitions/my_table?partitionColumn=date"
```

### With date range filter
```bash
curl "http://localhost:15003/api/partitions/my_table?startDate=2023-01-01&endDate=2023-01-31"
```

### With sub-partition filters
```bash
curl "http://localhost:15003/api/partitions/my_table?filter.region=us-west&filter.type=events"
```

## Response Format

```json
{
  "tableName": "my_database.my_table",
  "partitions": ["2023-01-01", "2023-01-02", "2023-01-03"],
  "success": true,
  "message": null
}
```

## Error Handling

The service handles errors gracefully and returns appropriate HTTP status codes:
- 200: Success
- 400: Bad request (invalid table name or parameters)
- 404: Endpoint not found
- 500: Internal server error

Error responses include details:
```json
{
  "tableName": "invalid_table",
  "partitions": [],
  "success": false,
  "message": "Error: Table not found"
}
```

## Integration with Existing Chronon Infrastructure

This module leverages existing Chronon components:
- `TableUtils` for partition listing logic
- `FormatProvider` for automatic table format detection
- Format-specific implementations (Hive, Iceberg, DeltaLake)
- Existing partition filtering and range support

## Configuration

The service uses Spark session configuration for partition column defaults:
```scala
spark.conf.set("spark.chronon.partition.column", "ds")
```

## Dependencies

- Apache Spark with Spark Connect support
- Netty for HTTP server
- Jackson for JSON serialization
- Existing Chronon catalog modules
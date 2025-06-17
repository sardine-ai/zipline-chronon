## Partition Listing logic

Given table and partitionSpec(partition_column: String, partition_format: String, ) of that table,

```python

# bigquery native catalog example
"spark.sql.catalog.lakehouse_output": "org.apache.iceberg.spark.SparkCatalog",
"spark.sql.catalog.lakehouse_output.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
"spark.sql.catalog.lakehouse_output.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
"spark.sql.catalog.lakehouse_output.warehouse": "<TODO>",
"spark.sql.catalog.lakehouse_output.gcp_location": "<TODO>",
"spark.sql.catalog.lakehouse_output.gcp_project": "<TODO>",

# Polaris catalog / iceberg rest catalog
"spark.sql.catalog.polaris_catalog": "org.apache.iceberg.spark.SparkCatalog",
"spark.sql.catalog.polaris_catalog.warehouse": "data_job_output",
"spark.sql.catalog.polaris_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
"spark.sql.catalog.polaris_catalog.uri": "<TODO>",
"spark.sql.catalog.polaris_catalog.credential": "<TODO>",
"spark.sql.catalog.polaris_catalog.scope": "PRINCIPAL_ROLE:ALL",

```

implement the following method

```
enum tableType = BIGQUERY_NATIVE | BIGQUERY_EXTERNAL_PARQUET | BIGQUERY_EXTERNAL_ICEBERG | REST_ICEBERG


def find_table_type(session: SparkSession, table: String, conf: Map[String, String]):
    #     
    
```

from ai.chronon.api.ttypes import Team
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "JOB_MODE": "local[*]",
            "CHRONON_ONLINE_CLASS": "[ONLINE-TODO]your.online.class",
            "CHRONON_ONLINE_ARGS": "[ONLINE-TODO]args prefixed with -Z become constructor map for your implementation of ai.chronon.online.Api, -Zkv-host=<YOUR_HOST> -Zkv-port=<YOUR_PORT>",
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "CUSTOMER_ID": "{{ customer_id }}",
            "GCP_PROJECT_ID": "<project-id>",
            "GCP_REGION": "<region>",
            "GCP_DATAPROC_CLUSTER_NAME": "<dataproc-cluster-name>",
            "GCP_BIGTABLE_INSTANCE_ID": "<bigtable-instance-id>",
        },
    ),
)


test = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {},
            RunMode.UPLOAD: {}
        }
    ),
)

team_conf = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={},
    ),
    conf=ConfigProperties(
        common={
            "spark.chronon.cloud_provider": "gcp",  # dummy test config
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.table.gcs.temporary_gcs_bucket": "zipline-warehouse-canary",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table.gcs.connector_output_dataset": "data",
            "spark.chronon.table.gcs.connector_output_project": "canary-443022",
            "spark.chronon.table_write.prefix": "gs://zipline-warehouse-canary/data/tables/",
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.catalog.spark_catalog.warehouse": "gs://zipline-warehouse-canary/data/tables/",
            "spark.sql.catalog.spark_catalog.gcp_location": "us-central1",
            "spark.sql.catalog.spark_catalog.gcp_project": "canary-443022",
            "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.spark_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.catalog.default_iceberg.warehouse": "gs://zipline-warehouse-canary/data/tables/",
            "spark.sql.catalog.default_iceberg.gcp_location": "us-central1",
            "spark.sql.catalog.default_iceberg.gcp_project": "canary-443022",
            "spark.sql.catalog.default_iceberg.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.default_iceberg": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.default_iceberg.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.chronon.backfill_cloud_provider": "gcp",  # dummy test config
            }
        }
    ),
)
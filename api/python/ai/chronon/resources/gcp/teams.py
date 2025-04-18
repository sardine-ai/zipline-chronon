from ai.chronon.api.ttypes import Team
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.table_write.format": "iceberg",

            "spark.sql.defaultCatalog": "bigquery_catalog",

            "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",

            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",

            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",

            # TODO: Please fill in the following values
            "spark.sql.catalog.bigquery_catalog.warehouse": "<gcs-prefix>",
            "spark.sql.catalog.bigquery_catalog.gcp_location": "<region>",
            "spark.sql.catalog.bigquery_catalog.gcp_project": "<project-id>",
            "spark.chronon.partition.format": "<date-format>", # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>", # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            "JOB_MODE": "local[*]",
            "CHRONON_ONLINE_CLASS": "[ONLINE-TODO]your.online.class",
            "CHRONON_ONLINE_ARGS": "[ONLINE-TODO]args prefixed with -Z become constructor map for your implementation of ai.chronon.online.Api, -Zkv-host=<YOUR_HOST> -Zkv-port=<YOUR_PORT>",

            # TODO: Fill in please
            "CUSTOMER_ID": "<customer_id>",
            "GCP_PROJECT_ID": "<project-id>",
            "GCP_REGION": "<region>",
            "GCP_DATAPROC_CLUSTER_NAME": "<dataproc-cluster-name>",
            "GCP_BIGTABLE_INSTANCE_ID": "<bigtable-instance-id>",
        },
    ),
)


test = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={},
        modeEnvironments={
            RunMode.BACKFILL: {},
            RunMode.UPLOAD: {}
        }
    ),
)

team_conf = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={},
    ),
)
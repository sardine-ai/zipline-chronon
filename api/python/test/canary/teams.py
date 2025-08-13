from ai.chronon.api.ttypes import Team
from ai.chronon.repo.cluster import generate_dataproc_cluster_config
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "CUSTOMER_ID": "dev",
            "GCP_PROJECT_ID": "canary-443022",
            "GCP_REGION": "us-central1",
            "GCP_DATAPROC_CLUSTER_NAME": "zipline-canary-cluster",
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",
            "FLINK_STATE_URI": "gs://zipline-warehouse-canary/flink-state",
            "FRONTEND_URL": "http://localhost:5173",
            # "FRONTEND_URL": https://34.111.151.47.nip.io/", # canary frontend URL
            "HUB_URL": "http://localhost:3903",
            # "HUB_URL": "http://34.133.227.246:3903/", # canary hub URL
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
            RunMode.BACKFILL: {
            },
            RunMode.UPLOAD: {
            }
        }
    ),
)

gcp = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "gcp",
            "CUSTOMER_ID": "dev",
            "GCP_PROJECT_ID": "canary-443022",
            "GCP_REGION": "us-central1",
            "GCP_DATAPROC_CLUSTER_NAME": "zipline-canary-cluster",
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",
            "ENABLE_PUBSUB": "true",
            "ARTIFACT_PREFIX": "gs://zipline-artifacts-dev",
            "CHRONON_ONLINE_ARGS": " -Ztasks=4",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
                "GCP_DATAPROC_CLUSTER_NAME": "zipline-transient-upload-cluster"
            }
        }
    ),
    conf=ConfigProperties(
        common={
            "spark.chronon.cloud_provider": "gcp",  # dummy test config
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.table.gcs.temporary_gcs_bucket": "zipline-warehouse-canary",
            "spark.chronon.partition.column": "ds",
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
            }
        }
    ),
    clusterConf=ClusterConfigProperties(
        modeClusterConfigs={
            RunMode.UPLOAD: {
                "dataproc.config": generate_dataproc_cluster_config(2, "canary-443022", "gs://zipline-artifacts-canary",
                                                                    worker_host_type="n2-highmem-4",
                                                                    master_host_type="n2-highmem-8")
            }
        }
    ),
)

aws = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "dev",
        }
    ),
)

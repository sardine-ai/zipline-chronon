from ai.chronon.api.ttypes import Team
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "_DATE",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "SERDE_CLASS": "your.serde.class",  # TODO : To decode data from kafka
            "SERDE_ARGS": "-Zkey1=<value1> -Zkey2=<value2>",  # TODO:will be passed to the constructor of your Serde Implmentation
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "CUSTOMER_ID": "canary",  # TODO: Customer ID
            "GCP_PROJECT_ID": "canary-443022",  # TODO: GCP Project ID
            "GCP_REGION": "us-central1",  # TODO: GCP Region
            "GCP_DATAPROC_CLUSTER_NAME": "canary-2",  # TODO: GCP Dataproc Cluster Name
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",  # TODO: GCP Bigtable Instance ID
        },
        modeEnvironments={
            RunMode.BACKFILL: {
                "EXECUTOR_CORES": "2",
                "DRIVER_MEMORY": "15G",
                "EXECUTOR_MEMORY": "4G",
                "PARALLELISM": "4",
                "MAX_EXECUTORS": "4",
            },
            RunMode.UPLOAD: {
                "PARALLELISM": "2",
                "MAX_EXECUTORS": "4",
            },
            RunMode.STREAMING: {
                "EXECUTOR_CORES": "2",
                "EXECUTOR_MEMORY": "4G",
                "PARTITIONS_PER_EXECUTOR": "2",
            },
        },
    ),
)


test = Team(
    outputNamespace="test",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "_test_column",
        }
    ),
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {
                "EXECUTOR_CORES": "2",
                "DRIVER_MEMORY": "15G",
                "EXECUTOR_MEMORY": "4G",
                "PARALLELISM": "4",
                "MAX_EXECUTORS": "4",
            },
            RunMode.UPLOAD: {
                "PARALLELISM": "2",
                "MAX_EXECUTORS": "4",
            },
        },
    ),
)


sample_team = Team(
    outputNamespace="test",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "_test_column_sample",
        }
    ),
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {
                "EXECUTOR_CORES": "2",
                "DRIVER_MEMORY": "15G",
                "EXECUTOR_MEMORY": "4G",
                "PARALLELISM": "4",
                "MAX_EXECUTORS": "4",
            },
            RunMode.UPLOAD: {
                "PARALLELISM": "2",
                "MAX_EXECUTORS": "4",
            },
        },
    ),
)

kaggle = Team(outputNamespace="kaggle")

quickstart = Team(outputNamespace="quickstart")

risk = Team(outputNamespace="risk")

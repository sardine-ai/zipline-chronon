from ai.chronon.api.ttypes import Team, EnvironmentVariables


default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
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
        backfill={  # Backfills tend to be larger than other jobs
            "EXECUTOR_CORES": "3",
            "DRIVER_MEMORY": "15G",
            "EXECUTOR_MEMORY": "24G",
            "PARALLELISM": "4000",
            "MAX_EXECUTORS": "1000",
        },
        upload={
            "EXECUTOR_CORES": "3",
            "EXECUTOR_MEMORY": "24G",
            "PARALLELISM": "1000",
            "MAX_EXECUTORS": "1000",
        },
        streaming={
            "EXECUTOR_CORES": "2",
            "EXECUTOR_MEMORY": "4G",
            "PARTITIONS_PER_EXECUTOR": "2",
        },
    ),
)


test = Team(
    namespace="test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        backfill={  # smaller jobs for testing
            "EXECUTOR_CORES": "2",
            "DRIVER_MEMORY": "15G",
            "EXECUTOR_MEMORY": "4G",
            "PARALLELISM": "4",
            "MAX_EXECUTORS": "4",
        },
        upload={
            "PARALLELISM": "2",
            "MAX_EXECUTORS": "4",
        },
    ),
)


sample_team = Team(
    namespace="test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        backfill={  # smaller jobs for testing
            "EXECUTOR_CORES": "2",
            "DRIVER_MEMORY": "15G",
            "EXECUTOR_MEMORY": "4G",
            "PARALLELISM": "4",
            "MAX_EXECUTORS": "4",
        },
        upload={
            "PARALLELISM": "2",
            "MAX_EXECUTORS": "4",
        },
    ),
)

etsy_search = Team(namespace="etsy-search")

kaggle = Team(namespace="kaggle")

quickstart = Team(namespace="quickstart")

risk = Team(namespace="risk")

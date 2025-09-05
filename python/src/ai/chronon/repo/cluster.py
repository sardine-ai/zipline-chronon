import json


def generate_dataproc_cluster_config(
    num_workers,
    project_id,
    artifact_prefix,
    master_host_type="n2-highmem-64",
    worker_host_type="n2-highmem-16",
    subnetwork="default",
    idle_timeout="7200s",
    initialization_actions=None,
    tags=None,
):
    """
    Create a configuration for a Dataproc cluster.
    :return: A json string representing the configuration.
    """
    if initialization_actions is None:
        initialization_actions = []
    return json.dumps(
        {
            "gceClusterConfig": {
                "subnetworkUri": subnetwork,
                "serviceAccount": "dataproc@" + project_id + ".iam.gserviceaccount.com",
                "serviceAccountScopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write",
                ],
                "metadata": {
                    "hive-version": "3.1.2",
                    "SPARK_BQ_CONNECTOR_URL": "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar",
                    "artifact_prefix": artifact_prefix.rstrip("/"),
                },
                "tags": tags or [],
            },
            "masterConfig": {
                "numInstances": 1,
                "machineTypeUri": master_host_type,
                "diskConfig": {"bootDiskType": "pd-standard", "bootDiskSizeGb": 1024},
            },
            "workerConfig": {
                "numInstances": num_workers,
                "machineTypeUri": worker_host_type,
                "diskConfig": {
                    "bootDiskType": "pd-standard",
                    "bootDiskSizeGb": 64,
                    "numLocalSsds": 2,
                },
            },
            "softwareConfig": {
                "imageVersion": "2.2.50-debian12",
                "optionalComponents": [
                    "FLINK",
                    "JUPYTER",
                ],
                "properties": {
                    "dataproc:dataproc.logging.stackdriver.enable": "true",
                    "dataproc:jobs.file-backed-output.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
                },
            },
            "initializationActions": [
                {"executable_file": initialization_action}
                for initialization_action in (
                    (initialization_actions or [])
                    + [artifact_prefix.rstrip("/") + "/scripts/copy_java_security.sh"]
                )
            ],
            "endpointConfig": {
                "enableHttpPortAccess": True,
            },
            "lifecycleConfig": {
                "idleDeleteTtl": idle_timeout,
            },
        }
    )


def fixed_cluster(
    size,
    project_id,
    artifact_prefix,
    subnetwork="default",
    initialization_actions=None,
    tags=None,
):
    """
    Create a Dataproc cluster configuration based on t-shirt sizes.
    
    :param size: T-shirt size - 'small', 'medium', or 'large'
    :param project_id: GCP project ID
    :param artifact_prefix: Artifact prefix for initialization scripts
    :param subnetwork: Subnetwork for the cluster
    :param initialization_actions: List of initialization actions
    :param tags: List of tags for the cluster
    :return: A json string representing the cluster configuration
    """
    size_configs = {
        'small': {
            'num_workers': 20,
            'worker_host_type': 'n2-highmem-4',  # 16GB, 4 cores
            'master_host_type': 'n2-highmem-4'   # Same as worker for consistency
        },
        'medium': {
            'num_workers': 50,
            'worker_host_type': 'n2-highmem-16',  # 32GB, 8 cores
            'master_host_type': 'n2-highmem-16'   # Same as worker for consistency
        },
        'large': {
            'num_workers': 250,
            'worker_host_type': 'n2-highmem-16', # 64GB, 16 cores  
            'master_host_type': 'n2-highmem-16'  # Same as worker for consistency
        }
    }
    
    if size not in size_configs:
        raise ValueError(f"Invalid size '{size}'. Must be one of: {list(size_configs.keys())}")
    
    config = size_configs[size]
    
    return generate_dataproc_cluster_config(
        num_workers=config['num_workers'],
        project_id=project_id,
        artifact_prefix=artifact_prefix,
        master_host_type=config['master_host_type'],
        worker_host_type=config['worker_host_type'],
        subnetwork=subnetwork,
        idle_timeout="3600s",  # 1 hour of inactivity
        initialization_actions=initialization_actions,
        tags=tags,
    )

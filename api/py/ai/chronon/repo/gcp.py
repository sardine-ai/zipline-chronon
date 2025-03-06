import base64
import logging
import multiprocessing
import os
from typing import List

from google.cloud import storage
import crcmod

from ai.chronon.repo.constants import ROUTES
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import (
    get_environ_arg,
    retry_decorator,
    DataprocJobType,
    get_customer_id,
    extract_filename_from_path,
    split_date_range,
    check_call,
    check_output,
)

# GCP DATAPROC SPECIFIC CONSTANTS
DATAPROC_ENTRY = "ai.chronon.integrations.cloud_gcp.DataprocSubmitter"
ZIPLINE_GCP_JAR_DEFAULT = "cloud_gcp_lib_deploy.jar"
ZIPLINE_GCP_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.cloud_gcp.GcpApiImpl"
ZIPLINE_GCP_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_GCP_SERVICE_JAR = "service_assembly_deploy.jar"


class GcpRunner(Runner):
    def __init__(self, args, jar_path):
        super().__init__(args, jar_path)

    @staticmethod
    def get_gcp_project_id() -> str:
        return get_environ_arg("GCP_PROJECT_ID")

    @staticmethod
    def get_gcp_bigtable_instance_id() -> str:
        return get_environ_arg("GCP_BIGTABLE_INSTANCE_ID")

    @staticmethod
    def get_gcp_region_id() -> str:
        return get_environ_arg("GCP_REGION")

    @staticmethod
    @retry_decorator(retries=2, backoff=5)
    def download_gcs_blob(bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        try:
            storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)
            print(
                "Downloaded storage object {} from bucket {} to local file {}.".format(
                    source_blob_name, bucket_name, destination_file_name
                )
            )
        except Exception as e:
            raise RuntimeError(f"Failed to download {source_blob_name}: {str(e)}")

    @staticmethod
    @retry_decorator(retries=2, backoff=5)
    def upload_gcs_blob(bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""

        try:
            storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)

            print(
                f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
            )
            return f"gs://{bucket_name}/{destination_blob_name}"
        except Exception as e:
            raise RuntimeError(f"Failed to upload {source_file_name}: {str(e)}")

    @staticmethod
    def get_gcs_file_hash(bucket_name: str, blob_name: str) -> str:
        """
        Get the hash of a file stored in Google Cloud Storage.
        """
        storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        if not blob:
            raise FileNotFoundError(
                f"File {blob_name} not found in bucket {bucket_name}"
            )

        return blob.crc32c

    @staticmethod
    def get_local_file_hash(file_path: str) -> str:
        """
        Calculate CRC32C hash of a local file.

        Args:
            file_path: Path to the local file

        Returns:
            Base64-encoded string of the file's CRC32C hash
        """
        crc32c_hash = crcmod.predefined.Crc("crc-32c")

        with open(file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(4096), b""):
                crc32c_hash.update(chunk)

        # Convert to base64 to match GCS format
        return base64.b64encode(crc32c_hash.digest()).decode("utf-8")

    @staticmethod
    def compare_gcs_and_local_file_hashes(
        bucket_name: str, blob_name: str, local_file_path: str
    ) -> bool:
        """
        Compare hashes of a GCS file and a local file to check if they're identical.

        Args:
            bucket_name: Name of the GCS bucket
            blob_name: Name/path of the blob in the bucket
            local_file_path: Path to the local file

        Returns:
            True if files are identical, False otherwise
        """
        try:
            gcs_hash = GcpRunner.get_gcs_file_hash(bucket_name, blob_name)
            local_hash = GcpRunner.get_local_file_hash(local_file_path)

            print(
                f"Local hash of {local_file_path}: {local_hash}. GCS file {blob_name} hash: {gcs_hash}"
            )

            return gcs_hash == local_hash

        except Exception as e:
            print(f"Error comparing files: {str(e)}")
            return False

    @staticmethod
    def download_zipline_dataproc_jar(
        destination_dir: str, customer_id: str, jar_name: str
    ):
        bucket_name = f"zipline-artifacts-{customer_id}"

        source_blob_name = f"jars/{jar_name}"
        destination_path = f"{destination_dir}/{jar_name}"

        are_identical = (
            GcpRunner.compare_gcs_and_local_file_hashes(
                bucket_name, source_blob_name, destination_path
            )
            if os.path.exists(destination_path)
            else False
        )

        if are_identical:
            print(f"{destination_path} matches GCS {bucket_name}/{source_blob_name}")
        else:
            print(
                f"{destination_path} does NOT match GCS {bucket_name}/{source_blob_name}"
            )
            print(f"Downloading {jar_name} from GCS...")

            GcpRunner.download_gcs_blob(bucket_name, source_blob_name, destination_path)
        return destination_path

    def generate_dataproc_submitter_args(
        self,
        user_args: str,
        job_type: DataprocJobType = DataprocJobType.SPARK,
        local_files_to_upload: List[str] = [],
    ):
        customer_warehouse_bucket_name = f"zipline-warehouse-{get_customer_id()}"

        gcs_files = []
        for source_file in local_files_to_upload:
            # upload to `metadata` folder
            destination_file_path = (
                f"metadata/{extract_filename_from_path(source_file)}"
            )
            gcs_files.append(
                GcpRunner.upload_gcs_blob(
                    customer_warehouse_bucket_name, source_file, destination_file_path
                )
            )

        # we also want the additional-confs included here. it should already be in the bucket

        zipline_artifacts_bucket_prefix = "gs://zipline-artifacts"

        gcs_files.append(
            f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}/confs/additional-confs.yaml"
        )

        gcs_file_args = ",".join(gcs_files)

        # include jar uri. should also already be in the bucket
        jar_uri = (
            f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}"
            + f"/jars/{ZIPLINE_GCP_JAR_DEFAULT}"
        )

        final_args = "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"

        if job_type == DataprocJobType.FLINK:
            main_class = "ai.chronon.flink.FlinkJob"
            flink_jar_uri = (
                f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}"
                + f"/jars/{ZIPLINE_GCP_FLINK_JAR_DEFAULT}"
            )
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --flink-main-jar-uri={flink_jar_uri}"
            )

        elif job_type == DataprocJobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --additional-conf-path=additional-confs.yaml --gcs-files={gcs_file_args}"
            )
        else:
            raise ValueError(f"Invalid job type: {job_type}")

    def run_dataproc_flink_streaming(self):
        user_args = {
            "--groupby-name": self.groupby_name,
            "--kafka-bootstrap": self.kafka_bootstrap,
            "--online-class": ZIPLINE_GCP_ONLINE_CLASS_DEFAULT,
            "-ZGCP_PROJECT_ID": GcpRunner.get_gcp_project_id(),
            "-ZGCP_BIGTABLE_INSTANCE_ID": GcpRunner.get_gcp_bigtable_instance_id(),
            "--savepoint-uri": self.savepoint_uri,
        }

        flag_args = {"--mock-source": self.mock_source}
        flag_args_str = " ".join(key for key, value in flag_args.items() if value)

        user_args_str = " ".join(
            f"{key}={value}" for key, value in user_args.items() if value
        )

        dataproc_args = self.generate_dataproc_submitter_args(
            job_type=DataprocJobType.FLINK,
            user_args=" ".join([user_args_str, flag_args_str]),
        )
        command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
        return command

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or self.mode == "fetch":
            entrypoint = "ai.chronon.online.fetcher.FetcherMain"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        elif self.mode in ["streaming", "streaming-client"]:
            # streaming mode
            command = self.run_dataproc_flink_streaming()
            command_list.append(command)
        else:
            if self.parallelism > 1:
                assert self.start_ds is not None and self.ds is not None, (
                    "To use parallelism, please specify --start-ds and --end-ds to "
                    "break down into multiple backfill jobs"
                )
                date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                for start_ds, end_ds in date_ranges:
                    user_args = ("{subcommand} {args} {additional_args}").format(
                        subcommand=ROUTES[self.conf_type][self.mode],
                        args=self._gen_final_args(
                            start_ds=self.start_ds,
                            end_ds=end_ds,
                            # overriding the conf here because we only want the
                            # filename, not the full path. When we upload this to
                            # GCS, the full path does get reflected on GCS. But
                            # when we include the gcs file path as part of dataproc,
                            # the file is copied to root and not the complete path
                            # is copied.
                            override_conf_path=(
                                extract_filename_from_path(self.conf)
                                if self.conf
                                else None
                            ),
                        ),
                        additional_args=os.environ.get(
                            "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                        ),
                    )
                    local_files_to_upload_to_gcs = []
                    if self.conf:
                        local_files_to_upload_to_gcs.append(self.conf)

                    dataproc_args = self.generate_dataproc_submitter_args(
                        local_files_to_upload=[self.conf],
                        # for now, self.conf is the only local file that requires uploading to gcs
                        user_args=user_args,
                    )
                    command = (
                        f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
                    )
                    command_list.append(command)
            else:
                user_args = ("{subcommand} {args} {additional_args}").format(
                    subcommand=ROUTES[self.conf_type][self.mode],
                    args=self._gen_final_args(
                        start_ds=self.start_ds,
                        # overriding the conf here because we only want the filename,
                        # not the full path. When we upload this to GCS, the full path
                        # does get reflected on GCS. But when we include the gcs file
                        # path as part of dataproc, the file is copied to root and
                        # not the complete path is copied.
                        override_conf_path=(
                            extract_filename_from_path(self.conf) if self.conf else None
                        ),
                    ),
                    additional_args=os.environ.get(
                        "CHRONON_CONFIG_ADDITIONAL_ARGS", ""
                    ),
                )
                local_files_to_upload_to_gcs = []
                if self.conf:
                    local_files_to_upload_to_gcs.append(self.conf)

                dataproc_args = self.generate_dataproc_submitter_args(
                    # for now, self.conf is the only local file that requires uploading to gcs
                    local_files_to_upload=local_files_to_upload_to_gcs,
                    user_args=user_args,
                )
                command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
                command_list.append(command)

        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                logging.info(
                    "Running args list {} with pool size {}".format(
                        command_list, self.parallelism
                    )
                )
                pool.map(check_call, command_list)
        elif len(command_list) == 1:
            output = check_output(command_list[0]).decode("utf-8").split("\n")
            print(*output, sep="\n")

            dataproc_submitter_id_str = "Dataproc submitter job id"

            dataproc_submitter_logs = [
                s for s in output if dataproc_submitter_id_str in s
            ]
            if dataproc_submitter_logs:
                log = dataproc_submitter_logs[0]
                job_id = log[
                    log.index(dataproc_submitter_id_str)
                    + len(dataproc_submitter_id_str)
                    + 1:
                ]
                try:
                    print(
                        """
                    <-----------------------------------------------------------------------------------
                    ------------------------------------------------------------------------------------
                                                      DATAPROC LOGS
                    ------------------------------------------------------------------------------------
                    ------------------------------------------------------------------------------------>
                    """
                    )
                    check_call(
                        f"gcloud dataproc jobs wait {job_id} --region={self.get_gcp_region_id()}"
                    )
                except Exception:
                    # swallow since this is just for tailing logs
                    pass

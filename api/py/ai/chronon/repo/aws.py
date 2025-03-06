import logging
import multiprocessing
import os
from typing import List

import boto3

from ai.chronon.repo.constants import ROUTES
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import DataprocJobType, get_customer_id, extract_filename_from_path, split_date_range, \
    check_call

# AWS SPECIFIC CONSTANTS
EMR_ENTRY = 'ai.chronon.integrations.aws.EmrSubmitter'
ZIPLINE_AWS_JAR_DEFAULT = 'cloud_aws_lib_deploy.jar'
ZIPLINE_AWS_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.aws.AwsApiImpl"
ZIPLINE_AWS_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_AWS_SERVICE_JAR = "service_assembly_deploy.jar"


class AwsRunner(Runner):
    def __init__(self, args, jar_path):
        super().__init__(args, jar_path)

    @staticmethod
    def upload_s3_file(bucket_name: str, source_file_name: str, destination_blob_name: str):
        """Uploads a file to the bucket."""
        obj = boto3.client("s3")
        try:
            obj.upload_file(source_file_name, bucket_name, destination_blob_name)
            print(
                f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
            )
            return f"s3://{bucket_name}/{destination_blob_name}"
        except Exception as e:
            raise RuntimeError(f"Failed to upload {source_file_name}: {str(e)}")


    @staticmethod
    def download_zipline_aws_jar(destination_dir: str, customer_id: str, jar_name: str):
        obj = boto3.client("s3")
        destination_path = f"{destination_dir}/{jar_name}"
        source_key_name = f"jars/{jar_name}"

        # Downloading a csv file
        # from S3 bucket to local folder
        obj.download_file(
            Filename=destination_path,
            Bucket=f"zipline-artifacts-{customer_id}",
            Key=source_key_name
        )
        print(f"Downloaded {jar_name} to {destination_path}")
        return destination_path


    def generate_emr_submitter_args(self, user_args: str, job_type: DataprocJobType = DataprocJobType.SPARK,
                                    local_files_to_upload: List[str] = []):
        customer_warehouse_bucket_name = f"zipline-warehouse-{get_customer_id()}"
        s3_files = []
        for source_file in local_files_to_upload:
            # upload to `metadata` folder
            destination_file_path = f"metadata/{extract_filename_from_path(source_file)}"
            s3_files.append(AwsRunner.upload_s3_file(
                customer_warehouse_bucket_name, source_file, destination_file_path))

        # we also want the additional-confs included here. it should already be in the bucket

        zipline_artifacts_bucket_prefix = 's3://zipline-artifacts'

        s3_files.append(
            f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}/confs/additional-confs.yaml")

        s3_file_args = ",".join(s3_files)

        # include jar uri. should also already be in the bucket
        jar_uri = f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}" + \
                  f"/jars/{ZIPLINE_AWS_JAR_DEFAULT}"

        final_args = "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"

        if job_type == DataprocJobType.FLINK:
            main_class = "ai.chronon.flink.FlinkJob"
            flink_jar_uri = f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}" \
                            + f"/jars/{ZIPLINE_AWS_FLINK_JAR_DEFAULT}"
            return final_args.format(
                user_args=user_args,
                jar_uri=jar_uri,
                job_type=job_type.value,
                main_class=main_class
            ) + f" --flink-main-jar-uri={flink_jar_uri}"

        elif job_type == DataprocJobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            return final_args.format(
                user_args=user_args,
                jar_uri=jar_uri,
                job_type=job_type.value,
                main_class=main_class
            ) + f" --additional-conf-path=additional-confs.yaml --files={s3_file_args}"
        else:
            raise ValueError(f"Invalid job type: {job_type}")


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
            raise ValueError("Streaming is not supported for AWS yet.")
        else:
            if self.parallelism > 1:
                assert self.start_ds is not None and self.ds is not None, (
                    "To use parallelism, please specify --start-ds and --end-ds to "
                    "break down into multiple backfill jobs"
                )
                date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                for start_ds, end_ds in date_ranges:
                    user_args = "{subcommand} {args} {additional_args}".format(
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
                    local_files_to_upload_to_aws = []
                    if self.conf:
                        local_files_to_upload_to_aws.append(self.conf)

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

                dataproc_args = self.generate_emr_submitter_args(
                    # for now, self.conf is the only local file that requires uploading to gcs
                    local_files_to_upload=local_files_to_upload_to_gcs,
                    user_args=user_args,
                )
                command = f"java -cp {self.jar_path} {EMR_ENTRY} {dataproc_args}"
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
            # TODO: add log tailing
            check_call(command_list[0])



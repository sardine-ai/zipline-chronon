package ai.chronon.integrations.aws

import ai.chronon.spark.SparkJob
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.services.emr.EmrClient
import ai.chronon.spark.JobSubmitterConstants._
import org.scalatest.BeforeAndAfterAll

class EmrSubmitterTest extends AnyFlatSpec with BeforeAndAfterAll {
//  "EmrSubmitterClient" should "return job id when a job is submitted" in {
//
//  }
//
//  it should "test flink job locally" ignore {
//
//  }
//
//  it should "test flink kafka ingest job locally" ignore {
//
//  }
//
  it should "Used to iterate locally. Do not enable this in CI/CD!" in {
    val emrSubmitter = new EmrSubmitter("canary",
                                        EmrClient
                                          .builder()
                                          .build())
    val jobId = emrSubmitter.submit(
      SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
      ),
      List("s3://zipline-artifacts-canary/additional-confs.yaml", "s3://zipline-warehouse-canary/purchases.v1"),
      "group-by-backfill",
      "--conf-path",
      "/mnt/zipline/purchases.v1",
      "--end-date",
      "2025-02-26",
      "--conf-type",
      "group_bys",
      "--additional-conf-path",
      "/mnt/zipline/additional-confs.yaml"
    )
    println("EMR job id: " + jobId)
    0
  }

}

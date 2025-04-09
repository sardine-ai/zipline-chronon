package ai.chronon.spark.submission

import ai.chronon.api
import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.spark.submission.JobSubmitterConstants.ConfTypeArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.LocalConfPathArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.OriginalModeArgKeyword
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import scala.reflect.ClassTag

sealed trait JobType
case object SparkJob extends JobType
case object FlinkJob extends JobType

trait JobSubmitter {

  def submit(jobType: JobType,
             submissionProperties: Map[String, String],
             jobProperties: Map[String, String],
             files: List[String],
             args: String*): String

  def status(jobId: String): Unit

  def kill(jobId: String): Unit
}

object JobSubmitter {

  def getArgValue(args: Array[String], argKeyword: String): Option[String] = {
    args
      .find(_.startsWith(argKeyword))
      .map(_.split("="))
      .map(_(1))
  }

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = true)

  def getModeConfigProperties(args: Array[String]): Option[Map[String, String]] = {
    val localConfPathValue = getArgValue(args, LocalConfPathArgKeyword)
    val confTypeValue = getArgValue(args, ConfTypeArgKeyword)

    val modeConfigProperties = if (localConfPathValue.isDefined && confTypeValue.isDefined) {
      val metadata = confTypeValue.get match {
        case "joins"           => parseConf[api.Join](localConfPathValue.get).metaData
        case "group_bys"       => parseConf[api.GroupBy](localConfPathValue.get).metaData
        case "staging_queries" => parseConf[api.StagingQuery](localConfPathValue.get).metaData
        case "models"          => parseConf[api.Model](localConfPathValue.get).metaData
        case _                 => throw new Exception("Invalid conf type")
      }

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.conf), originalMode) match {
          case (Some(conf), Some(mode)) =>
            val modeConfs = if (conf.isSetModeConfigs && conf.getModeConfigs.containsKey(mode)) {
              conf.getModeConfigs.get(mode).toScala
            } else if (conf.isSetCommon) {
              conf.getCommon.toScala
            } else {
              Map[String, String]()
            }
            Option(modeConfs)
          case _ => None
        }
      }
    } else None

    modeConfigProperties
  }
}

abstract class JobAuth {
  def token(): Unit = {}
}

object JobSubmitterConstants {
  val MainClass = "mainClass"
  val JarURI = "jarUri"
  val FlinkMainJarURI = "flinkMainJarUri"
  val SavepointUri = "savepointUri"
  val FlinkStateUri = "flinkStateUri"

  // EMR specific properties
  val ClusterInstanceCount = "clusterInstanceCount"
  val ClusterInstanceType = "clusterInstanceType"
  val ClusterIdleTimeout = "clusterIdleTimeout"
  val EmrReleaseLabel = "emrReleaseLabel"
  val ShouldCreateCluster = "shouldCreateCluster"
  val ClusterId = "jobFlowId"

  val JarUriArgKeyword = "--jar-uri"
  val JobTypeArgKeyword = "--job-type"
  val MainClassKeyword = "--main-class"
  val FlinkMainJarUriArgKeyword = "--flink-main-jar-uri"
  val FlinkSavepointUriArgKeyword = "--savepoint-uri"
  val FilesArgKeyword = "--files"
  val ConfTypeArgKeyword = "--conf-type"
  val LocalConfPathArgKeyword = "--local-conf-path"
  val OriginalModeArgKeyword = "--original-mode"

  val SharedInternalArgs: Set[String] = Set(
    JarUriArgKeyword,
    JobTypeArgKeyword,
    MainClassKeyword,
    FlinkMainJarUriArgKeyword,
    FlinkSavepointUriArgKeyword,
    LocalConfPathArgKeyword,
    OriginalModeArgKeyword,
    FilesArgKeyword
  )
}

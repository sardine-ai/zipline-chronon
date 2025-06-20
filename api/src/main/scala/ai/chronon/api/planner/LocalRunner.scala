package ai.chronon.api.planner

import ai.chronon.api.thrift.TBase
import ai.chronon.api.{Constants, MetaData, ThriftJsonCodec}
import ai.chronon.planner.{Node, NodeContent, SourceWithFilterNode}

import java.io.File
import scala.reflect.ClassTag
import scala.util.Try
import ai.chronon.api.PartitionSpec
import ai.chronon.api.Join
import ai.chronon.api.StagingQuery

object LocalRunner {

  private def listFiles(dir: String = "."): Seq[String] = {
    val baseDir = new File(dir)
    Option(baseDir.listFiles).getOrElse(Array()).flatMap { file =>
      if (file.isDirectory) listFiles(file.getPath)
      else Seq(file.getPath.replaceFirst("^\\./", ""))
    }
  }

  private def isIgnorableFile(path: String): Boolean = {
    val file = new File(path)
    Constants.extensionsToIgnore.exists(file.getName.endsWith) ||
    Constants.foldersToIgnore.exists(file.getPath.split("/").contains(_))
  }

  private def tryParsingConf[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[T] =
    try {
      Some(ThriftJsonCodec.fromJsonFile[T](file, check = false))
    } catch {
      case ex: Exception =>
        new RuntimeException(s"Failed to parse file: $file", ex).printStackTrace()
        None
    }

  def parseConfs[T <: TBase[_, _]: Manifest: ClassTag](confSubfolder: String): Seq[T] = listFiles(confSubfolder)
    .filterNot(isIgnorableFile)
    .flatMap(tryParsingConf[T])
    .toSeq

  /** bazel build //api:planner_deploy.jar
    * @param args
    */
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: LocalRunner <path_to_conf_subfolder> <conf_type>")
      System.exit(1)
    }
    val confSubfolder = args(0)
    val confType = args(1)
    implicit val testPartitionSpec = PartitionSpec.daily

    println("Parsed configurations:")
    confType match {
      case "joins" => {
        val confs = parseConfs[Join](confSubfolder)
        confs.map((c) => MonolithJoinPlanner(c)).map(_.buildPlan).foreach(println)
      }
      case "staging_queries" => {
        val confs = parseConfs[StagingQuery](confSubfolder)
        confs.map((c) => new StagingQueryPlanner(c)).map(_.buildPlan).foreach(println)
      }
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported conf type: $confType. Supported types are: joins, staging_queries."
        )
    }
  }

}

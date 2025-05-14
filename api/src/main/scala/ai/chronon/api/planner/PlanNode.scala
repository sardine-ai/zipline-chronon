package ai.chronon.api.planner

import ai.chronon.api.thrift.TBase
import ai.chronon.api.{Constants, MetaData, ThriftJsonCodec}

import java.io.File
import scala.reflect.ClassTag
import scala.util.Try

trait PlanNode {
  def metaData: MetaData
  def contents: Any
  def semanticHash: String
}

object PlanNode {

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

  def planConfs[T](confs: Seq[T], planner: Planner[T]): Seq[PlanNode] = ???
  def generatePlans(compiledFolder: String): Seq[PlanNode] = ???

}

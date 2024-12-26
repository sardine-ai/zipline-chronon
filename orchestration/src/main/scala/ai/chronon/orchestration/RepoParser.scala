package ai.chronon.orchestration

import ai.chronon.api
import ai.chronon.online.MetadataDirWalker.listFiles
import ai.chronon.online.MetadataDirWalker.parse
import ai.chronon.online.MetadataDirWalker.relativePath
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.Failure
import scala.util.Success

// parses a folder of
object RepoParser {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private def parseLogicalSet(dir: File): LogicalSet = {
    listFiles(dir).getValidFilesAndReport
      .foldLeft(LogicalSet()) { (logicalSet, file) =>
        val filePath = file.getPath

        val logicalSetTry = filePath match {
          case value if value.contains("joins/")           => parse[api.Join](file).map(logicalSet :+ _)
          case value if value.contains("group_bys/")       => parse[api.GroupBy](file).map(logicalSet :+ _)
          case value if value.contains("staging_queries/") => parse[api.StagingQuery](file).map(logicalSet :+ _)
          case value if value.contains("models/")          => parse[api.Model](file).map(logicalSet :+ _)

          case _ => Failure(new IllegalArgumentException(s"Unrecognized file path: $filePath"))
        }

        logicalSetTry match {
          case Success(value)     => logger.info(s"Parsed: ${relativePath(file)}"); value
          case Failure(exception) => logger.error(s"Failed to parse file: ${relativePath(file)}", exception); logicalSet
        }
      }
  }

  def main(args: Array[String]): Unit = {

    require(args.length == 1, "Usage: RepoParser <dir>")

    val dir = new File(args(0))
    require(dir.exists() && dir.isDirectory, s"Invalid directory: $dir")

    val logicalSet = parseLogicalSet(dir)
    val li = LineageIndex(logicalSet)

    println(li)
  }
}

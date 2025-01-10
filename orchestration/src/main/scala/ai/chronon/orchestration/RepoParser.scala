package ai.chronon.orchestration

import ai.chronon.online.MetadataDirWalker
import ai.chronon.orchestration.RepoTypes._
import org.apache.logging.log4j.scala.Logging

import java.io.File

// parses a folder of
object RepoParser extends App with Logging {

  private def readContent(file: File): FileContent = {
    val source = scala.io.Source.fromFile(file)
    val content = FileContent(source.mkString)
    source.close()
    content
  }

  def fileHashes(compileRoot: String): Map[Name, FileHash] = {
    val compileDir = new File(compileRoot)
    // list all files in compile root
    MetadataDirWalker
      .listFiles(new File(compileRoot))
      .getValidFilesAndReport
      .map { file =>
        val relativePath = compileDir.toPath.relativize(file.toPath)
        val name = Name(relativePath.toString)
        val content = readContent(file)
        val hash = content.hash
        name -> hash
      }
      .toMap
  }

  def fileContents(compileRoot: String, names: Seq[String]): Map[Name, FileContent] = {
    val compileDir = new File(compileRoot)
    names.map { file =>
      val name = Name(file)
      val relativePath = compileDir.toPath.resolve(file)
      val content = readContent(relativePath.toFile)
      name -> content
    }.toMap
  }

}

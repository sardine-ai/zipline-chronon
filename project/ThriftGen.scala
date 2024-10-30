import org.slf4j.LoggerFactory
import sbt.*

import scala.language.postfixOps
import sys.process.*

object Thrift {

  def print_and_execute(command: String): Int = {
    println(s"+ $command")
    try {
      val result = Process(command).!(ProcessLogger(
        out => println(s"[out] $out"),
        err => println(s"[err] $err")
      ))
      if (result != 0) {
        println(s"Command failed with exit code $result")
      }
      result
    } catch {
      case e: Exception =>
        println(s"Command failed with exception: ${e.getMessage}")
        throw e
    }
  }

  def replaceInFile(file: File): Unit = {
    val source = scala.io.Source.fromFile(file)
    val content = source.mkString
    source.close()
    val newContent = content.replace("org.apache.thrift", "ai.chronon.api.thrift")
    val writer = new java.io.PrintWriter(file)
    try {
      writer.write(newContent)
    } finally {
      writer.close()
    }
  }

  def gen(inputPath: String, outputPath: String, language: String, cleanupSuffixPath: String = "", extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $inputPath \ninto folder $outputPath" """ !;
    print_and_execute(s"rm -rf $outputPath/$cleanupSuffixPath")
    s"mkdir -p $outputPath" !;
    print_and_execute(s"thrift -version")
    print_and_execute(s"thrift --gen $language:generated_annotations=suppress -out $outputPath $inputPath")
    val javaFiles = (PathFinder(new File(s"$outputPath/ai/chronon/api/")) ** "*.java").get()
    javaFiles.foreach { file =>
      println(s"Processing file: ${file.getPath}")
      replaceInFile(file)
    }
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    println("\n")
    files
  }
}

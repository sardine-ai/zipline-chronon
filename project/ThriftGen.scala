import org.slf4j.LoggerFactory
import sbt.*

import scala.language.postfixOps
import sys.process.*

object ThriftGen {

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

  def gen(inputFolder: File, outputPath: String, language: String, extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift files at: $inputFolder/ \ninto folder $outputPath" """ !;
    print_and_execute(s"rm -rf $outputPath")
    print_and_execute(s"mkdir -p $outputPath");
    print_and_execute(s"thrift -version")
    val thriftFiles = (PathFinder(new File(s"$inputFolder/")) ** "*.thrift").get()
    thriftFiles.foreach { file =>
      println(s"Processing file: $file")
      print_and_execute(s"thrift --gen $language:generated_annotations=suppress -out $outputPath $file")
    }
    val javaFiles = (PathFinder(new File(s"$outputPath/ai/chronon/")) ** "*.java").get()
    javaFiles.foreach { file =>
      println(s"Processing file: ${file.getPath}")
      replaceInFile(file)
    }
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    println("\n")
    files
  }
}

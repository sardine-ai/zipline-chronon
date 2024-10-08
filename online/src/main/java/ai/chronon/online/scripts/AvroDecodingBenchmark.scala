package ai.chronon.online.scripts

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter

import java.io.File
import java.util
import scala.util.Random

// used for benchmarking decoding throughput
object AvroDecodingBenchmark {
  // Define the Avro schema
  val schema: Schema =
    new Schema.Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "RandomData",
      |  "fields": [
      |    {"name": "values", "type": {"type": "array", "items": "long", "size": 250}}
      |  ]
      |}
    """.stripMargin)

  private def generateAndEncode(): Unit = {
    val file = new File("random_data.avro")
    val datumWriter = new SpecificDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, file)

    val random = new Random()

    try {
      for (_ <- 1 to 1000000) { // Generate 1 million rows
        val record = new GenericData.Record(schema)
        val numList = new util.ArrayList[Long](250)
        for (i <- 0 until 250) {
          numList.add(random.nextLong())
        }
        record.put("values", numList)
        dataFileWriter.append(record)
      }
    } finally {
      dataFileWriter.close()
    }

    println("Data generation and encoding completed.")
  }

  private def measureDeserializationThroughput(): Unit = {
    val file = new File("random_data.avro")
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)

    val startTime = System.nanoTime()
    var count = 0L
    var totalBytes = 0L

    try {
      while (dataFileReader.hasNext) {
        val record = dataFileReader.next()
        count += 1
        totalBytes += record.toString.getBytes.length // Approximate size
      }
    } finally {
      dataFileReader.close()
    }

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9

    val recordsPerSecond = count / durationSeconds
    val bytesPerSecond = totalBytes / durationSeconds
    val megabytesPerSecond = bytesPerSecond / (1024 * 1024)
    val totalSizeMB = totalBytes.toDouble / (1024 * 1024)

    println("Deserialization throughput:")
    println(f"  Records: $count")
    println(f"  Total size: $totalSizeMB%.2f MB")
    println(f"  Duration: $durationSeconds%.2f seconds")
    println(f"  Throughput: $recordsPerSecond%.2f records/second")
    println(f"  Throughput: $megabytesPerSecond%.2f MB/second")
  }

  def main(args: Array[String]): Unit = {
    generateAndEncode()
    measureDeserializationThroughput()
  }
}

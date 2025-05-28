package ai.chronon.spark.ingest
import ai.chronon.api.{EngineType, PartitionRange}
import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.universe.runtimeMirror

abstract class DataImport {

  def sync(sourceTableName: String,
           destinationTableName: String,
           partitionRange: PartitionRange,
           query: Option[String])(implicit sparkSession: SparkSession): Unit

}

object DataImport {

  def from(engineType: EngineType): DataImport =
    try {

      val clazzName = engineType match {
        case EngineType.BIGQUERY => "ai.chronon.integrations.cloud_gcp.BigQueryImport"
        case _ =>
          throw new UnsupportedOperationException(
            s"Engine type ${engineType} is not supported for Staging Query export"
          )
      }

      val mirror = runtimeMirror(getClass.getClassLoader)
      val classSymbol = mirror.staticClass(clazzName)
      val classMirror = mirror.reflectClass(classSymbol)

      val constructor = classSymbol.primaryConstructor.asMethod
      val constructorMirror = classMirror.reflectConstructor(constructor)

      val reflected = constructorMirror()
      reflected.asInstanceOf[DataImport]

    } catch {

      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed to instantiate format provider. Please ensure the class is available in the classpath. Error: ${e.getMessage}",
          e
        )
    }

}

package ai.chronon.spark

import ai.chronon.api.DataPointer
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._

case class CatalogAwareDataPointer(inputTableOrPath: String, formatProvider: FormatProvider) extends DataPointer {

  override def tableOrPath: String = {
    formatProvider.resolveTableName(inputTableOrPath)
  }
  override lazy val options: Map[String, String] = {
    // Hack for now, include both read and write options for the datapointer.
    // todo(tchow): rework this abstraction. https://app.asana.com/0/1208785567265389/1209026103291854/f
    formatProvider.readFormat(inputTableOrPath).options ++ formatProvider.writeFormat(inputTableOrPath).options
  }

  override lazy val readFormat: Option[String] = {
    Option(formatProvider.readFormat(inputTableOrPath)).map(_.name)
  }

  override lazy val writeFormat: Option[String] = {
    Option(formatProvider.writeFormat(inputTableOrPath)).map(_.name)
  }

}

object DataPointer {

  def apply(tableOrPath: String, sparkSession: SparkSession): DataPointer = {
    val clazzName =
      sparkSession.conf.get("spark.chronon.table.format_provider.class", classOf[DefaultFormatProvider].getName)
    val mirror = runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(clazzName)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructor = classSymbol.primaryConstructor.asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)
    val reflected = constructorMirror(sparkSession)
    val provider = reflected.asInstanceOf[FormatProvider]

    CatalogAwareDataPointer(tableOrPath, provider)

  }

}

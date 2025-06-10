package ai.chronon.api

import ai.chronon.api.thrift.TBase
import scala.collection.JavaConverters._

import scala.reflect.ClassTag

sealed trait Validation {

  def validate[T <: TBase[_, _]: Manifest: ClassTag](confObj: T): Unit = {}
}

object JoinValidator extends Validation {

  override def validate[T <: TBase[_, _]: Manifest: ClassTag](confObj: T): Unit = {
    confObj match {
      case joinConf: Join =>
        require(Option(joinConf.metaData).nonEmpty, f"join metaData needs to be set for join ${joinConf}")
        require(Option(joinConf.metaData.name).nonEmpty, s"join.metaData.name needs to be set for join ${joinConf}")
        require(Option(joinConf.metaData.team).nonEmpty,
                s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")
        require(Option(joinConf.metaData.outputNamespace).nonEmpty,
                s"output namespace for joinConf ${joinConf.metaData.name} could not be empty or null")

        joinConf.joinParts.asScala.foreach((jp) => GroupByValidator.validate(jp.groupBy))
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid configuration object type: ${confObj.getClass.getName}. Expected Join type.")
    }
    // Implement validation logic for Join configurations
    // For example, check if required fields are present, types are correct, etc.
  }
}

object GroupByValidator extends Validation {

  override def validate[T <: TBase[_, _]: Manifest: ClassTag](confObj: T): Unit = {
    confObj match {
      case gbConf: GroupBy =>
        require(Option(gbConf.metaData).nonEmpty, s"groupBy metaData needs to be set for groupBy ${gbConf}")
        require(Option(gbConf.metaData.name).nonEmpty, s"groupBy.metaData.name needs to be set for groupBy ${gbConf}")
        require(Option(gbConf.metaData.team).nonEmpty,
                s"groupBy.metaData.team needs to be set for joinPart ${gbConf.metaData.name}")
    }
  }
}

object StagingQueryValidator extends Validation {

  override def validate[T <: TBase[_, _]: Manifest: ClassTag](confObj: T): Unit = {
    // Implement validation logic for StagingQuery configurations
    // For example, check if required fields are present, types are correct, etc.
  }
}

object ConfValidator {

  def validate[T <: TBase[_, _]: Manifest: ClassTag](confType: String, confObj: T): Unit = {
    confType match {
      case "joins"           => JoinValidator.validate(confObj)
      case "group_bys"       => GroupByValidator.validate(confObj)
      case "staging_queries" => StagingQueryValidator.validate(confObj)
      case "models"          => ()
      case _ =>
        throw new IllegalArgumentException(
          s"Unable to validate configuration due to invalid confType $confType"
        )
    }
  }
}

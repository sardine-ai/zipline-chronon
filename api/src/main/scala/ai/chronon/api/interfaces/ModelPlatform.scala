package ai.chronon.api.interfaces
import ai.chronon.api.{Model, PartitionSpec}

class ModelPlatform {

  // should produce model artifact at the output path specified by model metadata
  def train(modelConf: Model, date: String, outputPath: String): Unit

  def deployEndpoint()

}

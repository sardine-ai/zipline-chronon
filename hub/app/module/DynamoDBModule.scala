package module

import com.google.inject.AbstractModule
import ai.chronon.integrations.aws.AwsApiImpl
import play.api.{Configuration, Environment}

class DynamoDBModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[AwsApiImpl]).asEagerSingleton()
  }
}

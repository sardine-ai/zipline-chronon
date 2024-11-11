package ai.chronon.integrations.aws

import ai.chronon.online.Api
import ai.chronon.online.ExternalSourceRegistry
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import ai.chronon.online.LoggableResponse
import ai.chronon.online.Serde
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.net.URI

/**
  * Implementation of Chronon's API interface for AWS. This is a work in progress and currently just covers the
  * DynamoDB based KV store implementation.
  */
class AwsApiImpl(conf: Map[String, String]) extends Api(conf) {
  @transient lazy val ddbClient: DynamoDbClient = {
    val regionEnvVar =
      sys.env.getOrElse("AWS_DEFAULT_REGION", throw new IllegalArgumentException("Missing AWS_DEFAULT_REGION env var"))
    val dynamoEndpoint =
      sys.env.getOrElse("DYNAMO_ENDPOINT", throw new IllegalArgumentException("Missing DYNAMO_ENDPOINT env var"))

    DynamoDbClient
      .builder()
      .region(Region.of(regionEnvVar))
      .endpointOverride(URI.create(dynamoEndpoint)) // TODO remove post docker
      .build()
  }

  override def genKvStore: KVStore = {
    new DynamoDBKVStoreImpl(ddbClient)
  }

  /**
    * The stream decoder method in the AwsApi is currently unimplemented. This needs to be implemented before
    * we can spin up the Aws streaming Chronon stack
    */
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): Serde = ???

  /**
    * The external registry extension is currently unimplemented. We'll need to implement this prior to spinning up
    * a fully functional Chronon serving stack in Aws
    * @return
    */
  override def externalRegistry: ExternalSourceRegistry = ???

  /**
    * The logResponse method is currently unimplemented. We'll need to implement this prior to bringing up the
    * fully functional serving stack in Aws which includes logging feature responses to a stream for OOC
    */
  override def logResponse(resp: LoggableResponse): Unit = ???
}

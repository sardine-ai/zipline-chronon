package ai.chronon.integrations.aws

import ai.chronon.online.{Api, ExternalSourceRegistry, GroupByServingInfoParsed, KVStore, LoggableResponse, Serde}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

/**
  * Implementation of Chronon's API interface for AWS. This is a work in progress and currently just covers the
  * DynamoDB based KV store implementation.
  */
class AwsApiImpl(conf: Map[String, String]) extends Api(conf) {
  val ddbClient: DynamoDbClient = {
    val regionEnvVar = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")
    val accessKeyId = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "fakeaccesskey")
    val secretAccessKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "fakesecretaccesskey")
    val dynamoEndpoint = sys.env.getOrElse("DYNAMO_ENDPOINT", "http://dynamo:8000")

    val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)

    DynamoDbClient
      .builder()
      .region(Region.of(regionEnvVar))
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .endpointOverride(URI.create(dynamoEndpoint)) // TODO remove post docker
      .build()
  }

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): Serde = ???

  override def genKvStore: KVStore = {
    new DynamoDBKVStoreImpl(ddbClient)
  }

  override def externalRegistry: ExternalSourceRegistry = ???

  override def logResponse(resp: LoggableResponse): Unit = ???
}

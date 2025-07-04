package ai.chronon.flink.test

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

object UserAvroSchema {
  val schema: Schema = SchemaBuilder
    .record("User")
    .namespace("com.example")
    .doc("Test User Schema")
    .fields()
    .name("id")
    .doc("A unique identifier")
    .`type`()
    .intType()
    .noDefault()
    .name("username")
    .doc("The user's username")
    .`type`()
    .stringType()
    .noDefault()
    .name("tags")
    .doc("List of tags associated with the user")
    .`type`()
    .array()
    .items()
    .stringType()
    .noDefault()
    .name("address")
    .doc("User's address information")
    .`type`()
    .record("AddressRecord")
    .fields()
    .name("street")
    .`type`()
    .stringType()
    .noDefault()
    .name("city")
    .`type`()
    .stringType()
    .noDefault()
    .name("country")
    .`type`()
    .stringType()
    .noDefault()
    .name("postalCode")
    .`type`()
    .unionOf()
    .nullType()
    .and()
    .stringType()
    .endUnion()
    .nullDefault()
    .endRecord()
    .noDefault()
    .name("preferences")
    .doc("User preferences stored as key-value pairs")
    .`type`()
    .map()
    .values()
    .stringType()
    .noDefault()
    .name("lastLoginTimestamp")
    .doc("Timestamp of last login in milliseconds since epoch")
    .`type`()
    .longType()
    .noDefault()
    .name("isActive")
    .doc("Whether the user account is active")
    .`type`()
    .booleanType()
    .booleanDefault(true)
    .endRecord()
}

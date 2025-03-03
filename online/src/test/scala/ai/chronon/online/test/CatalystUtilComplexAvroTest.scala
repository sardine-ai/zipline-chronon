package ai.chronon.online.test

import ai.chronon.api.{StructType => ChrononStructType}
import ai.chronon.online.{AvroConversions, CatalystUtil}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.AvroCatalystUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.{Base64, UUID, HashMap => JHashMap}
import scala.collection.JavaConverters._

// This test sanity checks large schemas like beacon top that have
// a few hundred fields and confirms that we are able to run the catalyst expression through them without issues
class CatalystUtilComplexAvroTest extends AnyFlatSpec {
  import BeaconTopPayloadGenerator._

  val selects: Seq[(String, String)] = Map(
    "favorite" -> "IF(event_name = 'backend_favorite_item2', 1, 0)",
    "listing_id" -> "EXPLODE(TRANSFORM(SPLIT(COALESCE(properties_top.sold_listing_ids, properties_top.listing_id), ','), e -> CAST(e AS LONG)))",
    "ts" -> "timestamp",
    "add_cart" -> "IF(event_name = 'backend_add_to_cart', 1, 0)",
    "purchase" -> "IF(event_name = 'backend_cart_payment', 1, 0)",
    "view" -> "IF(event_name = 'view_listing',  1, 0)"
  ).toSeq

  val wheres: Seq[String] = Seq(
    "event_name in ('backend_add_to_cart', 'view_listing', 'backend_cart_payment', 'backend_favorite_item2')",
    "( (properties_top.gdpr_p in ('1', '3') AND properties_top.gdpr_tp in ('1', '3')) OR ((NOT properties_top.gdpr_p IS NOT NULL) AND " +
      "(NOT properties_top.gdpr_tp IS NOT NULL) AND properties_top.region in ('US', 'CA', 'AU', 'MX', 'JP', 'NZ', 'BR', 'CN') AND event_logger = 'native' AND event_source in ('ios', 'android')) )",
    "properties_top.isBot IS NULL OR properties_top.isBot != 'true'",
    "properties_top.isSupportLogin IS NULL OR properties_top.isSupportLogin != 'true'",
    // existing predicate that doesn't work
    // "( (NOT properties_top.isBot = 'true') AND (NOT properties_top.isSupportLogin = 'true') )"
  )

  def processEvent(base64Payload: String): Seq[Map[String, Any]] = {
    val payloadBytes = java.util.Base64.getDecoder.decode(base64Payload)

    val encoder = AvroCatalystUtils.buildEncoder(beaconTopSchema.toString)
    val sparkRowDeser = encoder.asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()
    val avroDeserializer = AvroCatalystUtils.buildAvroDataToCatalyst(beaconTopSchema.toString)
    val internalRow = avroDeserializer.nullSafeEval(payloadBytes).asInstanceOf[InternalRow]
    val sparkRow = sparkRowDeser(internalRow)
    val chrononSchema =
      AvroConversions.toChrononSchema(beaconTopSchema).asInstanceOf[ChrononStructType]
    val eventExprEncoder = encoder.asInstanceOf[ExpressionEncoder[Row]]
    val rowSerializer = eventExprEncoder.createSerializer()
    val cu = new CatalystUtil(chrononSchema, selects, wheres)
    val catalystInternalRow = rowSerializer(sparkRow)
    cu.performSql(catalystInternalRow)
  }

  it should "successfully deser real beacon payload" in {
    val beaconTopPayload =
      "Jmxpc3RpbmdfaW1hZ2Vfc3dpcGWi5NzBn2UCODNEMTAxNEIxRjAzRDQxMjJBMzVCQzkwNEU0MTYASEExQUEyNEI2LTRFQzMtNDZGMS1BRTZDLTc3NzdGQzQ5QUE4OUhERkUzQjI0QS01MjI0LTRDQzktQkY1NC1DNzhDOEQ1Q0EyQ0QMbmF0aXZlBmlvcxo2OC4yMjYuMTQzLjMwlAJNb3ppbGxhLzUuMCAoaVBob25lOyBDUFUgaVBob25lIE9TIDE4XzFfMSBsaWtlIE1hYyBPUyBYKSBBcHBsZVdlYktpdC82MDUuMS4xNSAoS0hUTUwsIGxpa2UgR2Vja28pIE1vYmlsZS8xNUUxNDggRXRzeUluYy83LjEyIHJ2OjcxMjAwLjgwLjAA2gJldHN5Oi8vc2NyZWVuL2Jyb3dzZWxpc3RpbmdzP3JlZj1wYWdlLTIqbG9jYXRpb24tMTEqaXRlbXNfcGVyX3BhZ2UtMzYqcXVlcnktcnVzdGljJTIwd2VkZGluZyUyMGNha2UlMjBjdXR0ZXIqaXNfYWQtMCpjc2x1Zy00ZGY0ZDE0MGM2OThjZWY0ZTg3NDAwZmFkMjc3MGE2NTAzN2E5MjQwOjY1NTQwMjE4MgIEBmZ2ZRgxNzM5MDUyOTgyLjAcZXRhbGFfb3ZlcnJpZGVMMC4zRDEwMTRCMUYwM0Q0MTIyQTM1QkM5MDRFNDE2LjAuMC4wLjAAAAK6pYMlAgACAAIAAAAAAAL+x9vBn2UCBBhhY3RpdmVfaW5kZXgCMRRudW1faW1hZ2VzBDExAAACCmVuLVVTAAAAAAAAAAAAAAAAAAIkMTczOTM5NTQ3OC40Mjc4MjY5AAIiMTY0NjYxNDEyNy4wMjk1MDECDkV0c3lJbmMCIjE2NDY2MTQxMjcuMDI5NTAxAgxhY3RpdmUCJDcuMTIgcnY6NzEyMDAuODAuMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIYdmlld19saXN0aW5nAAAAAAAAAAAAAAAAAiQxNzM5Mzk1NDc4LjQwNzc2MjECJDE3MzkzOTU0NzguNjc2MTEyMgIUMTczOTM5NTQ3OAAAAgZpT1MCDDE4LjEuMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAjRmcmFuei1wcm9kLTdmNWNjYzY3Yi1xczl4YgAAAAAAAgIzAgIzAAAAAAAAAAAAAAIUaVBob25lMTIsMQISaVBob25lIDExAAAAAAACMkV0c3lMaXN0aW5nVmlld0NvbnRyb2xsZXIAAAIIbnVsbAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCHRydWUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgZVU0QAAAAAAAAAAAAAAAAAAAAAAAAAAAIIV2lmaQAAAAAAAAACEHBvcnRyYWl0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCmVuLVVTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgRVUwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgoyMjUyOQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAh5BbWVyaWNhL0NoaWNhZ28AAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    val result = processEvent(beaconTopPayload)
    assert(result.isEmpty) // no rows should be returned as the event is not in the where clause
  }

  it should "matches event_name condition (backend_add_to_cart)" in {
    val addToCartEvent = createModifiedBeacon(Map(
      "event_name" -> "backend_add_to_cart",
      "event_logger" -> "native",
      "event_source" -> "ios",
      "properties_top.region" -> "US"
    ))
    val payloadBase64 = serializeToBase64(addToCartEvent)
    val result = processEvent(payloadBase64)
    assert(result.nonEmpty) // expect a result row here
  }

  it should "match event_name condition (view_listing) with GDPR consent" in {
    val viewListingWithGdpr = createModifiedBeacon(Map(
      "event_name" -> "view_listing",
      "properties_top.gdpr_p" -> "1",
      "properties_top.gdpr_tp" -> "1"
    ))
    val payloadBase64 = serializeToBase64(viewListingWithGdpr)
    val result = processEvent(payloadBase64)
    assert(result.nonEmpty) // expect a result row here
  }

  it should "match event_name condition (backend_cart_payment) with regional condition" in {
    val purchaseEventRegional = createModifiedBeacon(Map(
      "event_name" -> "backend_cart_payment",
      "event_logger" -> "native",
      "event_source" -> "android",
      "properties_top.region" -> "CA"
    ))
    val payloadBase64 = serializeToBase64(purchaseEventRegional)
    val result = processEvent(payloadBase64)
    assert(result.nonEmpty) // expect a result row here
  }

  it should "match event_name condition (backend_favorite_item2) with listing IDs" in {
    val favoriteEvent = createModifiedBeacon(Map(
      "event_name" -> "backend_favorite_item2",
      "event_logger" -> "native",
      "event_source" -> "ios",
      "properties_top.region" -> "JP",
      "properties_top.listing_id" -> "789012,456789"
    ))
    val payloadBase64 = serializeToBase64(favoriteEvent)
    val result = processEvent(payloadBase64)
    assert(result.nonEmpty) // expect a result row here
  }

  it should "NOT match (bot flag is true)" in {
    val botEvent = createModifiedBeacon(Map(
      "event_name" -> "view_listing",
      "event_logger" -> "native",
      "event_source" -> "android",
      "properties_top.region" -> "US",
      "properties_top.isBot" -> "true"
    ))
    val payloadBase64 = serializeToBase64(botEvent)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (support login is true)" in {
    val supportLoginEvent = createModifiedBeacon(Map(
      "event_name" -> "backend_add_to_cart",
      "event_logger" -> "native",
      "event_source" -> "ios",
      "properties_top.region" -> "MX",
      "properties_top.isSupportLogin" -> "true"
    ))
    val payloadBase64 = serializeToBase64(supportLoginEvent)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (wrong event_name)" in {
    val wrongEventName = createModifiedBeacon(Map(
      "event_name" -> "search",
      "event_logger" -> "native",
      "event_source" -> "ios",
      "properties_top.region" -> "US"
    ))
    val payloadBase64 = serializeToBase64(wrongEventName)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (incompatible region & not GDPR)" in {
    val wrongRegion = createModifiedBeacon(Map(
      "event_name" -> "view_listing",
      "event_logger" -> "native",
      "event_source" -> "ios",
      "properties_top.region" -> "UK"
    ))
    val payloadBase64 = serializeToBase64(wrongRegion)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }
}

object BeaconTopPayloadGenerator {
  val beaconTopSchema: Schema = new Schema.Parser().parse(
    """{"type":"record","name":"BeaconTop","namespace":"com.etsy","fields":[{"name":"event_name","type":"string"},{"name":"timestamp","type":"long"},{"name":"browser_id","type":["null","string"],"default":null},{"name":"primary_event","type":"boolean"},{"name":"guid","type":"string"},{"name":"page_guid","type":"string"},{"name":"event_logger","type":"string"},{"name":"event_source","type":"string"},{"name":"ip","type":"string"},{"name":"user_agent","type":"string"},{"name":"loc","type":"string"},{"name":"ref","type":"string"},{"name":"cookies","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"ab","type":["null",{"type":"map","values":{"type":"array","items":"string"}}],"default":null},{"name":"user_id","type":["null","long"],"default":null},{"name":"isMobileRequest","type":["null","boolean"],"default":null},{"name":"isMobileDevice","type":["null","boolean"],"default":null},{"name":"isMobileTemplate","type":["null","boolean"],"default":null},{"name":"detected_currency_code","type":["null","string"],"default":null},{"name":"detected_language","type":["null","string"],"default":null},{"name":"detected_region","type":["null","string"],"default":null},{"name":"listing_ids","type":["null",{"type":"array","items":"long"}],"default":null},{"name":"event_timestamp","type":["null","long"],"default":null},{"name":"properties","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"properties_top","type":{"type":"record","name":"BeaconTopProperties","fields":[{"name":"gdpr_p","type":["null","string"],"default":null},{"name":"gdpr_tp","type":["null","string"],"default":null},{"name":"region","type":["null","string"],"default":null},{"name":"isBot","type":["null","string"],"default":null},{"name":"isSupportLogin","type":["null","string"],"default":null},{"name":"listing_id","type":["null","string"],"default":null},{"name":"sold_listing_ids","type":["null","string"],"default":null}]}}]}"""
  )

  // Create writer for serializing records
  val writer = new SpecificDatumWriter[GenericRecord](beaconTopSchema)

  // Function to create a base BeaconTop record
  def createBaseBeaconTop(): GenericRecord = {
    val record = new GenericData.Record(beaconTopSchema)

    // Set default values
    record.put("event_name", "view_listing")
    record.put("timestamp", Instant.now().toEpochMilli)
    record.put("browser_id", "test-browser-id")
    record.put("primary_event", true)
    record.put("guid", s"test-guid-${UUID.randomUUID().toString.take(8)}")
    record.put("page_guid", s"test-page-guid-${UUID.randomUUID().toString.take(8)}")
    record.put("event_logger", "web")
    record.put("event_source", "web")
    record.put("ip", "127.0.0.1")
    record.put("user_agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    record.put("loc", "https://etsy.com/listing/123456")
    record.put("ref", "")

    // Create cookies map
    val cookies = new JHashMap[String, String]()
    cookies.put("test-cookie", "test-value")
    record.put("cookies", cookies)

    // Create A/B test map
    val ab = new JHashMap[String, java.util.List[String]]()
    ab.put("test-experiment", List("variant_a").asJava)
    record.put("ab", ab)

    record.put("user_id", 12345L)
    record.put("isMobileRequest", false)
    record.put("isMobileDevice", false)
    record.put("isMobileTemplate", false)
    record.put("detected_currency_code", "USD")
    record.put("detected_language", "en-US")
    record.put("detected_region", "US")

    // Create listing_ids array
    val listingIds = new GenericData.Array[Long](2, beaconTopSchema.getField("listing_ids").schema().getTypes.get(1))
    listingIds.add(123456L)
    listingIds.add(789012L)
    record.put("listing_ids", listingIds)

    record.put("event_timestamp", Instant.now().toEpochMilli)

    // Create properties map
    val properties = new JHashMap[String, String]()
    properties.put("test-prop", "test-value")
    record.put("properties", properties)

    // Create properties_top record
    record.put("properties_top", createDefaultPropertiesTop())

    record
  }

  // Function to create default properties_top record
  def createDefaultPropertiesTop(): GenericRecord = {
    val propertiesSchema = beaconTopSchema.getField("properties_top").schema()
    val props = new GenericData.Record(propertiesSchema)

    // Set all fields to null by default
    props.put("gdpr_p", null)
    props.put("gdpr_tp", null)
    props.put("region", "US")

    props.put("isBot", null)
    props.put("isSupportLogin", null)

    props.put("listing_id", "123456")
    props.put("sold_listing_ids", null)

    props
  }

  // Create a modified beacon with specific property changes
  def createModifiedBeacon(modifications: Map[String, Any]): GenericRecord = {
    val record = createBaseBeaconTop()

    modifications.foreach { case (key, value) =>
      if (key.contains('.')) {
        // Handle nested properties
        val Array(parent, child) = key.split('.')
        val parentRecord = record.get(parent).asInstanceOf[GenericRecord]
        parentRecord.put(child, value)
      } else {
        record.put(key, value)
      }
    }

    record
  }

  // Serialize a record to Avro binary and encode as Base64
  def serializeToBase64(record: GenericRecord): String = {
    val baos = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)

    writer.write(record, encoder)
    encoder.flush()

    Base64.getEncoder.encodeToString(baos.toByteArray)
  }
}

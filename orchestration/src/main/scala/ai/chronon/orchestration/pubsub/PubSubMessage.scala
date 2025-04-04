package ai.chronon.orchestration.pubsub

import ai.chronon.orchestration.DummyNode
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage

/** Base message interface for Pub/Sub messages.
  *
  * This trait defines a common interface for messages across different Pub/Sub
  * implementations, providing a platform-agnostic way to work with message
  * data and attributes.
  *
  * The interface separates:
  * - Message metadata (attributes/properties)
  * - Message payload (data/body)
  *
  * This abstraction allows the system to:
  * - Work with different message formats consistently
  * - Hide implementation-specific details from business logic
  * - Support multiple Pub/Sub providers with a unified interface
  * - Test message handling without depending on actual Pub/Sub implementations
  */
trait PubSubMessage {

  /** Gets the message attributes/properties as key-value pairs.
    *
    * Message attributes are metadata associated with the message that provide
    * additional context or routing information.
    *
    * @return A map of attribute names to their string values
    */
  def getAttributes: Map[String, String]

  /** Gets the message payload/body data.
    *
    * The message data contains the actual content being delivered. It may be
    * empty for messages that only use attributes for signaling.
    *
    * @return The binary message data, if present
    */
  def getData: Option[Array[Byte]]
}

/** Google Cloud-specific extension of the PubSubMessage interface.
  */
trait GcpPubSubMessage extends PubSubMessage {

  /** Converts this message to the Google Cloud Pub/Sub native format.
    *
    * This method transforms the abstract message representation into the
    * concrete Google Pub/Sub format required by the Google Cloud libraries.
    *
    * @return A Google Cloud PubsubMessage ready for publishing
    */
  def toPubsubMessage: PubsubMessage
}

/** Message implementation for job submission requests.
  *
  * This class represents a message that triggers the execution of a node
  * in the computation graph. It implements the Google Cloud Pub/Sub message
  * interface, allowing it to be published to Google Cloud Pub/Sub topics.
  *
  * @param nodeName The name of the node to execute
  * @param data Optional message body as a string
  * @param attributes Additional key-value metadata for the message
  */
case class JobSubmissionMessage(
    nodeName: String,
    data: Option[String] = None,
    attributes: Map[String, String] = Map.empty
) extends GcpPubSubMessage {

  /** Gets the combined message attributes including the node name.
    *
    * This implementation ensures the node name is always included in the attributes
    * map, even if it wasn't explicitly provided in the constructor.
    *
    * @return Map containing the node name and any additional attributes
    */
  override def getAttributes: Map[String, String] = {
    attributes + ("nodeName" -> nodeName)
  }

  /** Gets the message data as a UTF-8 encoded byte array.
    *
    * Converts the optional string data to a byte array using UTF-8 encoding.
    *
    * @return The message data as bytes, if present
    */
  override def getData: Option[Array[Byte]] = {
    data.map(_.getBytes("UTF-8"))
  }

  /** Converts this message to a native Google Cloud Pub/Sub message.
    *
    * This method builds a Google Pub/Sub message with:
    * - The node name as a required attribute
    * - Any additional attributes provided to this message
    * - Optional message data encoded as UTF-8
    *
    * @return A properly formatted Google Cloud PubsubMessage
    */
  override def toPubsubMessage: PubsubMessage = {
    val builder = PubsubMessage
      .newBuilder()
      .putAttributes("nodeName", nodeName)

    // Add additional attributes
    attributes.foreach { case (key, value) =>
      builder.putAttributes(key, value)
    }

    // Add message data if provided
    data.foreach { d =>
      builder.setData(ByteString.copyFromUtf8(d))
    }

    builder.build()
  }
}

/** Factory methods for creating JobSubmissionMessage instances.
  *
  * This companion object provides convenient factory methods for creating
  * JobSubmissionMessage instances from different sources.
  *
  * TODO: The conversion from DummyNode is temporary and will be replaced
  * with more appropriate conversion methods in the future.
  */
object JobSubmissionMessage {

  /** Creates a JobSubmissionMessage from a DummyNode.
    *
    * This is a temporary method for backward compatibility.
    *
    * @param node The DummyNode to create a message for
    * @return A JobSubmissionMessage for the node
    * @deprecated Use fromNodeName instead
    */
  def fromDummyNode(node: DummyNode): JobSubmissionMessage = {
    JobSubmissionMessage(
      nodeName = node.name,
      data = Some(s"Job submission for node: ${node.name}")
    )
  }
}

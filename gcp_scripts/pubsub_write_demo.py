from avro.io import BinaryEncoder, DatumWriter
import avro.schema as schema
import io
import json
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding

project_id = "double-operator-430521-j5"
topic_id = "transactions"
avsc_file = "/Users/varantzanoyan/repos/chronon/gcp_scripts/transactions.avro"

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

# Prepare to write Avro records to the binary output stream.
with open(avsc_file, "rb") as file:
    avro_schema = schema.parse(file.read())
writer = DatumWriter(avro_schema)
bout = io.BytesIO()

# Prepare some data using a Python dictionary that matches the Avro schema
record = {
    "user": 1,
    "merchant": 123,
    "is_refund": False,
    "amount": 19.99
}

try:
    # Get the topic encoding type.
    topic = publisher_client.get_topic(request={"topic": topic_path})
    encoding = topic.schema_settings.encoding

    # Encode the data according to the message serialization type.
    if encoding == Encoding.BINARY:
        encoder = BinaryEncoder(bout)
        writer.write(record, encoder)
        data = bout.getvalue()
        print(f"Preparing a binary-encoded message:\n{data.decode()}")
    elif encoding == Encoding.JSON:
        data_str = json.dumps(record)
        print(f"Preparing a JSON-encoded message:\n{data_str}")
        data = data_str.encode("utf-8")
    else:
        print(f"No encoding specified in {topic_path}. Abort.")
        exit(0)

    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

except NotFound:
    print(f"{topic_id} not found.")

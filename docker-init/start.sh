#!/bin/bash
if ! python3.8 generate_anomalous_data.py; then
    echo "Error: Failed to generate anomalous data" >&2
    exit 1
fi

# Load up metadata into DynamoDB
echo "Loading metadata.."
if ! java -cp /app/cli/spark.jar:$CLASSPATH ai.chronon.spark.Driver metadata-upload --conf-path=/chronon_sample/production/ --online-jar=/app/cli/cloud_aws.jar --online-class=ai.chronon.integrations.aws.AwsApiImpl; then
  echo "Error: Failed to load metadata into DynamoDB" >&2
  exit 1
fi
echo "Metadata load completed successfully!"

# Add these java options as without them we hit the below error:
# throws java.lang.ClassFormatError accessible: module java.base does not "opens java.lang" to unnamed module @36328710
export JAVA_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
exec "./hub/bin/hub"

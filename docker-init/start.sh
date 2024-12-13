#!/bin/bash

start_time=$(date +%s)
if ! python3.8 generate_anomalous_data.py; then
    echo "Error: Failed to generate anomalous data" >&2
    exit 1
else
    end_time=$(date +%s)
    elapsed_time=$((end_time - start_time))
    echo "Anomalous data generated successfully! Took $elapsed_time seconds."
fi


# Check if required JAR files exist
if [[ ! -f $SPARK_JAR ]] || [[ ! -f $CLOUD_AWS_JAR ]]; then
    echo "Error: Required JAR files not found" >&2
    exit 1
fi

# Load up metadata into DynamoDB
echo "Loading metadata.."
if ! java -cp $SPARK_JAR:$CLASSPATH ai.chronon.spark.Driver metadata-upload --conf-path=/chronon_sample/production/ --online-jar=$CLOUD_AWS_JAR --online-class=$ONLINE_CLASS; then
  echo "Error: Failed to load metadata into DynamoDB" >&2
  exit 1
fi
echo "Metadata load completed successfully!"

# Initialize DynamoDB
echo "Initializing DynamoDB Table .."
if ! output=$(java -cp $SPARK_JAR:$CLASSPATH ai.chronon.spark.Driver create-summary-dataset \
  --online-jar=$CLOUD_AWS_JAR \
  --online-class=$ONLINE_CLASS 2>&1); then
  echo "Error: Failed to bring up DynamoDB table" >&2
  echo "Java command output: $output" >&2
  exit 1
fi
echo "DynamoDB Table created successfully!"


start_time=$(date +%s)

# Add these java options as without them we hit the below error:
# throws java.lang.ClassFormatError accessible: module java.base does not "opens java.lang" to unnamed module @36328710
export JAVA_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
exec java -jar hub-0.1.0-SNAPSHOT.jar run ai.chronon.hub.HubVerticle -Dserver.port=9000 -Dai.chronon.metrics.host=$STATSD_HOST -conf config.json

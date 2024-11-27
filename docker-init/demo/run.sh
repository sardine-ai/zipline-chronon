#!/bin/bash

# Stop and remove existing container
if docker ps -a | grep -q spark-app; then
  docker stop spark-app || echo "Failed to stop container"
  docker rm spark-app || echo "Failed to remove container"
fi

SPARK_JAR_PATH="${SPARK_JAR_PATH:-$HOME/repos/chronon/spark/target/scala-2.12}"
LOG_CONFIG_FILE="${LOG_CONFIG_FILE:-$HOME/repos/chronon/docker-init/demo/log4j2.properties}"

if [ ! -d "$SPARK_JAR_PATH" ]; then
  echo "Error: JAR directory not found: $SPARK_JAR_PATH"
  exit 1
fi

# Run new container
docker run -d \
  --name spark-app \
  -v "$SPARK_JAR_PATH":/opt/chronon/jars \
  -v "$LOG_CONFIG_FILE":/opt/chronon/log4j2.properties \
  obs

# Submit with increased memory
docker exec spark-app \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --driver-memory 8g \
  --conf "spark.driver.maxResultSize=6g" \
  --conf "spark.driver.memory=8g" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:/opt/chronon/log4j2.properties" \
  --driver-class-path "/opt/spark/jars/*:/opt/chronon/jars/*" \
  --conf "spark.driver.host=localhost" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --class ai.chronon.spark.scripts.ObservabilityDemo \
  /opt/chronon/jars/spark-assembly-0.1.0-SNAPSHOT.jar
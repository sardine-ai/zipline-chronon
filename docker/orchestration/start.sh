#!/bin/bash
set -e

# Required environment variables
required_vars=("ORCHESTRATION_JAR" "ORCHESTRATION_PORT")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

JMX_OPTS="-XX:MaxMetaspaceSize=1g -XX:MaxRAMPercentage=70.0 -XX:MinRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:MaxHeapFreeRatio=100 -XX:MinHeapFreeRatio=0"

temporal server start-dev \
  --db-filename /tmp/temporal.db \
  --ui-ip 0.0.0.0 \
  --headless=false \
  --ip 0.0.0.0 &

java -jar pgadapter.jar \
  -p $GCP_PROJECT_ID \
  -i $DB_INSTANCE \
  -d $DB_NAME \
  -s 5432 &

  # Wait for Temporal to be ready
  while ! temporal operator cluster health | grep -q "SERVING"; do
    echo "Waiting for Temporal to start..."
    sleep 2
  done

if ! java -jar $ORCHESTRATION_JAR run ai.chronon.orchestration.service.OrchestrationVerticle \
  $JMX_OPTS \
  -Dserver.port=$ORCHESTRATION_PORT; then
  echo "Error: Fetcher service failed to start"
  exit 1
fi
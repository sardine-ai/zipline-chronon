#!/bin/bash
set -e

# Required environment variables
required_vars=("FETCHER_JAR" "FETCHER_PORT")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

if [[ $USE_AWS == true ]]; then
  ONLINE_JAR=$CLOUD_AWS_JAR
  ONLINE_CLASS=$AWS_ONLINE_CLASS
else
  ONLINE_JAR=$CLOUD_GCP_JAR
  ONLINE_CLASS=$GCP_ONLINE_CLASS
fi

if [ -z "$EXPORTER_OTLP_ENDPOINT" ]; then
  echo "OpenTelemetry endpoint not configured. Disabling metrics reporting"
  METRICS_ENABLED="false"
else
  METRICS_ENABLED="true"
fi

JMX_OPTS="-XX:MaxMetaspaceSize=1g -XX:MaxRAMPercentage=70.0 -XX:MinRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:MaxHeapFreeRatio=100 -XX:MinHeapFreeRatio=0"

echo "Starting Fetcher service with online jar $ONLINE_JAR and online class $ONLINE_CLASS"

if ! java -jar $FETCHER_JAR run ai.chronon.service.FetcherVerticle \
  $JMX_OPTS \
  -Dserver.port=$FETCHER_PORT \
  -Donline.jar=$ONLINE_JAR \
  -Dai.chronon.metrics.enabled=$METRICS_ENABLED \
  -Dai.chronon.metrics.exporter.url=$EXPORTER_OTLP_ENDPOINT \
  -Donline.class=$ONLINE_CLASS; then
  echo "Error: Fetcher service failed to start"
  exit 1
fi

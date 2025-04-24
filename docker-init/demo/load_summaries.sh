# Kick off the ObsDemo spark job in the app container

docker compose -f docker-init/compose.yaml exec app /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --driver-memory 8g \
  --conf "spark.driver.maxResultSize=6g" \
  --conf "spark.driver.memory=8g" \
  --driver-class-path "/opt/spark/jars/*:/app/cli/*" \
  --conf "spark.driver.host=localhost" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --class ai.chronon.spark.scripts.ObservabilityDemoDataLoader \
  /app/cli/spark.jar

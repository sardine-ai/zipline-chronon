# Use the bitnami spark image to pull the relevan Spark jars for the CLI
FROM bitnami/spark:3.5.2 AS spark-source

# Start a new stage for the runtime image
# We expect some build artifacts to be pre-populated by running the docker-init/build.sh (or manually triggered sbt commands)
FROM amazoncorretto:17

# Install python deps
COPY docker-init/requirements.txt .
# Python3.8 to allow for numpy==1.22.0 due to security concern
RUN amazon-linux-extras enable python3.8
RUN yum clean metadata; yum install -y python38 unzip
RUN  pip3.8 install --upgrade pip; pip3.8 install -r requirements.txt
ENV PYSPARK_PYTHON=python3.8

RUN mkdir -p /app/cli

COPY docker-init/generate_anomalous_data.py /app/
COPY docker-init/start.sh /start.sh
RUN chmod +x /start.sh
WORKDIR /app

# Copy api examples
COPY ./api/py/test/sample /chronon_sample

# COPY cli jars
COPY ./docker-init/build-artifacts/spark_assembly_deploy.jar /app/cli/spark.jar
COPY ./docker-init/build-artifacts/cloud_aws_lib_deploy.jar /app/cli/cloud_aws.jar
COPY ./api/py/ai/chronon/repo/run.py /app/cli/
ENV CHRONON_DRIVER_JAR="/app/cli/spark.jar"

# Set up Spark dependencies to help with launching CLI
# Copy Spark JARs from the Bitnami image
COPY --from=spark-source /opt/bitnami/spark/jars /opt/spark/jars
COPY --from=spark-source /opt/bitnami/spark/bin /opt/spark/bin

# Add all Spark JARs to the classpath
ENV CLASSPATH=/opt/spark/jars/*

# Copy frontend + play zipped dist and set up app directory structure
COPY ./docker-init/build-artifacts/hub_assembly_deploy.jar /app
COPY ./docker-init/hub/config.json /app

EXPOSE 9000

ENTRYPOINT ["/start.sh"]

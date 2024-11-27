# Use the first Dockerfile to set up the build environment
FROM base_image:latest AS build_env

# Copy your application source code
COPY . /app

# Build a fat jar for the various sub-modules
RUN sbt clean && sbt assembly

# Build the frontend and play app
RUN sbt dist

# Use the bitnami spark image to pull the relevan Spark jars for the CLI
FROM bitnami/spark:3.5.2 AS spark-source

# Start a new stage for the runtime image
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
COPY --from=build_env /app/api/py/test/sample /chronon_sample

# COPY cli jars
COPY --from=build_env /app/spark/target/scala-2.12/spark-assembly-0.1.0-SNAPSHOT.jar /app/cli/spark.jar
COPY --from=build_env /app/cloud_aws/target/scala-2.12/cloud_aws-assembly-0.1.0-SNAPSHOT.jar /app/cli/cloud_aws.jar
COPY --from=build_env /app/api/py/ai/chronon/repo/run.py /app/cli/
ENV CHRONON_DRIVER_JAR="/app/cli/spark.jar"

# Set up Spark dependencies to help with launching CLI
# Copy Spark JARs from the Bitnami image
COPY --from=spark-source /opt/bitnami/spark/jars /opt/spark/jars
COPY --from=spark-source /opt/bitnami/spark/bin /opt/spark/bin

# Add all Spark JARs to the classpath
ENV CLASSPATH=/opt/spark/jars/*

# Copy frontend + play zipped dist and set up app directory structure
COPY --from=build_env /app/hub/target/universal/hub-0.1.0-SNAPSHOT.zip /app
RUN unzip hub-0.1.0-SNAPSHOT.zip -d /app/hub && \
    cp -r hub/hub-0.1.0-SNAPSHOT/* hub/. && \
    rm -rf hub/hub-0.1.0-SNAPSHOT hub-0.1.0-SNAPSHOT.zip

EXPOSE 9000

ENTRYPOINT ["/start.sh"]

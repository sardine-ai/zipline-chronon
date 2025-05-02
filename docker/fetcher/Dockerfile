# Start from a Debian base image
FROM openjdk:17-jdk-slim

# We expect jars to be copied to the build_output directory as docker can't read from bazel-bin as that's a symlink
# https://stackoverflow.com/questions/31881904/docker-follow-symlink-outside-context
ENV CLOUD_AWS_JAR_PATH=build_output/cloud_aws_lib_deploy.jar
ENV CLOUD_GCP_JAR_PATH=build_output/cloud_gcp_lib_deploy.jar
ENV FETCHER_SVC_JAR_PATH=build_output/service_assembly_deploy.jar
ENV FETCHER_LAUNCH_SCRIPT=docker/fetcher/start.sh
ENV GCP_ONLINE_CLASS=ai.chronon.integrations.cloud_gcp.GcpApiImpl
ENV AWS_ONLINE_CLASS=ai.chronon.integrations.aws.AwsApiImpl

# Update package lists and install necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-dev \
    python3-setuptools \
    vim \
    wget \
    procps \
    python3-pip

ENV SCALA_VERSION 2.12.18

RUN curl https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb -k -o scala.deb && \
    apt install -y ./scala.deb && \
    rm -rf scala.deb /var/lib/apt/lists/*

ENV SCALA_HOME="/usr/bin/scala"
ENV PATH=${PATH}:${SCALA_HOME}/bin

WORKDIR /srv/zipline

ENV CLOUD_AWS_JAR=${CLOUD_AWS_JAR:-"/srv/zipline/cloud_aws/cloud_aws.jar"}
ENV CLOUD_GCP_JAR=${CLOUD_GCP_JAR:-"/srv/zipline/cloud_gcp/cloud_gcp.jar"}
ENV FETCHER_JAR=${FETCHER_JAR:-"/srv/zipline/fetcher/service.jar"}
ENV LOG_PATH=${LOG_PATH:-"/srv/zipline/fetcher/logs"}

COPY $CLOUD_AWS_JAR_PATH "$CLOUD_AWS_JAR"
COPY $CLOUD_GCP_JAR_PATH "$CLOUD_GCP_JAR"
COPY $FETCHER_SVC_JAR_PATH "$FETCHER_JAR"
COPY $FETCHER_LAUNCH_SCRIPT /srv/zipline/fetcher/start.sh

ENV FETCHER_PORT=9000

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$FETCHER_PORT/ping || exit 1

RUN mkdir -p $LOG_PATH && \
    chmod 755 $LOG_PATH

CMD /srv/zipline/fetcher/start.sh


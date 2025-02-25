# Start from a Debian base image
FROM openjdk:17-jdk-slim

# Set this manually before building the image, requires a local build of the jar

ENV CHRONON_JAR_PATH=build_output/spark_assembly_deploy.jar
ENV CLOUD_AWS_JAR_PATH=build_output/cloud_aws_lib_deploy.jar
ENV CLOUD_GCP_JAR_PATH=build_output/cloud_gcp_lib_deploy.jar
ENV FETCHER_SVC_JAR_PATH=build_output/service_assembly_deploy.jar

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

ENV THRIFT_VERSION 0.21.0
ENV SCALA_VERSION 2.12.18

# Install thrift
RUN curl -sSL "http://archive.apache.org/dist/thrift/$THRIFT_VERSION/thrift-$THRIFT_VERSION.tar.gz" -o thrift.tar.gz \
       && mkdir -p /usr/src/thrift \
       && tar zxf thrift.tar.gz -C /usr/src/thrift --strip-components=1 \
       && rm thrift.tar.gz \
       && cd /usr/src/thrift \
       && ./configure  --without-python --without-cpp \
       && make \
       && make install \
       && cd / \
       && rm -rf /usr/src/thrift

RUN curl https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb -k -o scala.deb && \
    apt install -y ./scala.deb && \
    rm -rf scala.deb /var/lib/apt/lists/*

ENV SCALA_HOME="/usr/bin/scala"
ENV PATH=${PATH}:${SCALA_HOME}/bin

## Download spark and hadoop dependencies and install

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV SPARK_VERSION=${SPARK_VERSION:-"3.5.1"}
ENV HADOOP_VERSION=${HADOOP_VERSION:-"3"}
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
RUN mkdir -p /opt/spark/spark-events
WORKDIR ${SPARK_HOME}


RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz \
 && tar xvzf spark.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark.tgz

# Add some additional custom jars for other connectors like BigTable etc
RUN mkdir -p /opt/custom-jars && \
    curl -L "https://repo1.maven.org/maven2/com/google/cloud/spark/bigtable/spark-bigtable_2.12/0.2.1/spark-bigtable_2.12-0.2.1.jar" \
    -o /opt/custom-jars/spark-bigtable_2.12-0.2.1.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/2.20.0/log4j-slf4j-impl-2.20.0.jar" \
    -o /opt/custom-jars/log4j-slf4j-impl-2.20.0.jar

# Install python deps
COPY quickstart/requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"

COPY quickstart/conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:/srv/chronon/:$PYTHONPATH

# If trying a standalone docker cluster
WORKDIR ${SPARK_HOME}
# If doing a regular local spark box.
WORKDIR /srv/chronon

ENV DRIVER_JAR_PATH="/srv/spark/spark_embedded.jar"
ENV CLOUD_AWS_JAR=${CLOUD_AWS_JAR:-"/srv/cloud_aws/cloud_aws.jar"}
ENV CLOUD_GCP_JAR=${CLOUD_GCP_JAR:-"/srv/cloud_gcp/cloud_gcp.jar"}
ENV FETCHER_JAR=${FETCHER_JAR:-"/srv/fetcher/service.jar"}

COPY api/py/test/sample ./
COPY quickstart/mongo-online-impl /srv/onlineImpl
COPY $CHRONON_JAR_PATH "$DRIVER_JAR_PATH"
COPY $CLOUD_AWS_JAR_PATH "$CLOUD_AWS_JAR"
COPY $CLOUD_GCP_JAR_PATH "$CLOUD_GCP_JAR"
COPY $FETCHER_SVC_JAR_PATH "$FETCHER_JAR"

ENV CHRONON_DRIVER_JAR="$DRIVER_JAR_PATH"

ENV SPARK_SUBMIT_OPTS="\
-XX:MaxMetaspaceSize=1024m \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

CMD tail -f /dev/null
FROM openjdk:17-jdk-slim
ARG GCP_REGION
ARG GCP_PROJECT_ID
ARG CUSTOMER_ID

ENV GCP_REGION=${GCP_REGION}
ENV GCP_PROJECT_ID=${GCP_PROJECT_ID}
ENV CUSTOMER_ID=${CUSTOMER_ID}
ENV DB_INSTANCE=orchestration-main
ENV DB_NAME=execution-info


ENV ORCHESTRATION_SVC_JAR_PATH=build-outputs/orchestration_assembly_deploy.jar
ENV ORCHESTRATION_LAUNCH_SCRIPT=docker/orchestration/start.sh

ENV ORCHESTRATION_JAR=/srv/zipline/orchestration/service.jar

ENV SCALA_VERSION=2.12.18

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

RUN curl https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb -k -o scala.deb && \
    apt install -y ./scala.deb && \
    rm -rf scala.deb /var/lib/apt/lists/*

ENV SCALA_HOME="/usr/bin/scala"
ENV PATH=${PATH}:${SCALA_HOME}/bin

WORKDIR /srv/zipline
COPY $ORCHESTRATION_LAUNCH_SCRIPT /srv/zipline/orchestration/start.sh
COPY $ORCHESTRATION_SVC_JAR_PATH "$ORCHESTRATION_JAR"

RUN wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz \
    && tar -xzvf pgadapter.tar.gz

RUN curl -sSf https://temporal.download/cli.sh | sh
ENV PATH="/root/.temporalio/bin:${PATH}"

ENV ORCHESTRATION_PORT=3903

COPY build-outputs/application_default_credentials.json /srv/zipline/orchestration/credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/srv/zipline/orchestration/credentials.json

EXPOSE 7233 8233 3903 5432

CMD /srv/zipline/orchestration/start.sh
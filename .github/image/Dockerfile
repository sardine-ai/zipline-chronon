FROM alpine:latest

# build using command: docker build --progress=plain -t chronon-base .

# Install necessary tools and Python
RUN apk add --no-cache wget curl bash python3 py3-pip

RUN apk add --no-cache openjdk-17-jdk

# java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# sbt for scala
RUN curl -L "https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz" | tar -xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:${PATH}"

# thrift
ARG THRIFT_VERSION=0.21.0
RUN apk add --no-cache \
        build-base \
        cmake \
        boost-dev \
        openssl-dev \
        libevent-dev \
        bison \
        flex \
        autoconf \
        automake \
        libtool \
        curl && \
    curl -LSs https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz -o thrift-${THRIFT_VERSION}.tar.gz && \
    tar -xzf thrift-${THRIFT_VERSION}.tar.gz && \
    cd thrift-${THRIFT_VERSION} && \
    ./configure --without-python --without-cpp --without-nodejs --without-java && \
    make && \
    make install && \
    cd .. && \
    rm -rf thrift-${THRIFT_VERSION} thrift-${THRIFT_VERSION}.tar.gz && \
    apk del \
        build-base \
        cmake \
        boost-dev \
        openssl-dev \
        libevent-dev \
        bison \
        flex \
        autoconf \
        automake \
        libtool \
        curl && \
    rm -rf /var/cache/apk/*

# Upgrade pip and install some common Python packages
RUN python3 -m pip install --break-system-packages --upgrade pip && \
    pip3 install --break-system-packages pytest tox flake8

# spark uses snappy to write files, snappy needs gcompat
RUN apk add gcompat

# Verify installations
RUN java -version && \
    thrift -version && \
    python3 --version  && \
    pip3 --version

# Set working directory
WORKDIR /app

# Cmd to run when starting the container
CMD ["/bin/bash"]

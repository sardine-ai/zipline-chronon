FROM alpine:latest

# build using command: docker build --progress=plain -t chronon-base .

# Install necessary tools and Python
RUN apk add --no-cache wget curl bash python3 py3-pip

# Add Amazon Corretto repository
RUN wget -O /etc/apk/keys/amazoncorretto.rsa.pub https://apk.corretto.aws/amazoncorretto.rsa.pub && \
    echo "https://apk.corretto.aws/" >> /etc/apk/repositories && \
    apk update
RUN apk add --no-cache amazon-corretto-17

# java
ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH $PATH:$JAVA_HOME/bin

# sbt for scala
RUN curl -L "https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz" | tar -xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:${PATH}"

# thrift (0.20.0 at the time of writing this)
RUN apk add --no-cache thrift

# Upgrade pip and install some common Python packages
RUN python3 -m pip install --break-system-packages --upgrade pip && \
    pip3 install --break-system-packages pytest tox flake8

# Verify installations
RUN java -version && \
    thrift -version && \
    python3 --version  && \
    pip3 --version

# Set working directory
WORKDIR /app

# Command to run when starting the container
CMD ["/bin/bash"]
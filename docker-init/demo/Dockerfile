FROM apache/spark:3.5.3-scala2.12-java17-ubuntu

# Switch to root to install Java 17
USER root

# Install Amazon Corretto 17
RUN apt-get update && \
    apt-get install -y wget software-properties-common gnupg2 && \
    wget -O- https://apt.corretto.aws/corretto.key --https-only | apt-key add - && \
    add-apt-repository 'deb https://apt.corretto.aws stable main' && \
    apt-get update && \
    apt-get install -y java-17-amazon-corretto-jdk && \
    update-alternatives --set java /usr/lib/jvm/java-17-amazon-corretto/bin/java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directory and set appropriate permissions
RUN mkdir -p /opt/chronon/jars && \
    chown -R 185:185 /opt/chronon && \
    chmod 755 /opt/chronon/jars

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$PATH:$JAVA_HOME/bin

# Switch back to spark user
USER 185

# Set environment variables for Spark classpath
ENV SPARK_CLASSPATH="/opt/spark/jars/*"
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*:/opt/chronon/jars/*"
ENV HADOOP_CLASSPATH="/opt/spark/jars/*"

CMD ["tail", "-f", "/dev/null"]
# Use the first Dockerfile to set up the build environment
FROM base_image:latest AS build_env

# Copy your application source code
COPY . /app

# Build the frontend and play app
RUN sbt dist

# Start a new stage for the runtime image
FROM amazoncorretto:17

 # Install python deps
COPY docker-init/requirements.txt .
RUN yum install -y python3 unzip
RUN  pip3 install --upgrade pip; pip3 install -r requirements.txt

RUN mkdir -p /app
COPY docker-init/generate_anomalous_data.py /app/
COPY docker-init/start.sh /start.sh
RUN chmod +x /start.sh
WORKDIR /app

# Copy frontend + play zipped dist and set up app directory structure
COPY --from=build_env /app/hub/target/universal/hub-0.1.0-SNAPSHOT.zip /app
RUN unzip hub-0.1.0-SNAPSHOT.zip -d /app/hub && \
    cp -r hub/hub-0.1.0-SNAPSHOT/* hub/. && \
    rm -rf hub/hub-0.1.0-SNAPSHOT hub-0.1.0-SNAPSHOT.zip

EXPOSE 9000

ENTRYPOINT ["/start.sh"]

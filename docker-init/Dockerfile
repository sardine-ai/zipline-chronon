FROM python:3.9.8-slim-buster AS py3

# ARG SPARK_VERSION=3.5.2

# Setup the directories for our Spark and Hadoop installations
# ENV HOMEDIR="/app"

# RUN mkdir -p ${HOMEDIR}
# WORKDIR ${HOMEDIR}


FROM openjdk:8-slim-buster

COPY --from=py3 / /


 # Install python deps
COPY requirements.txt .
RUN pip3 install --upgrade pip; pip3 install -r requirements.txt

COPY generate_anomalous_data.py . 

ENV DYNAMO_ENDPOINT="http://localhost:8000"
ENV AWS_DEFAULT_REGION="fakeregion"
ENV AWS_ACCESS_KEY_ID="fakeaccesskey"
ENV AWS_SECRET_ACCESS_KEY="fakesecretkey"

CMD ["python3", "generate_anomalous_data.py"]
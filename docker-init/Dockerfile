FROM python:3.11-slim-buster AS py3
FROM openjdk:11-slim-buster

COPY --from=py3 / /


 # Install python deps
COPY requirements.txt .
RUN pip3 install --upgrade pip; pip3 install -r requirements.txt

COPY generate_anomalous_data.py . 

ENV DYNAMO_ENDPOINT="http://localhost:8000"
ENV AWS_DEFAULT_REGION="fakeregion"
ENV AWS_ACCESS_KEY_ID="fakeaccesskey"
ENV AWS_SECRET_ACCESS_KEY="fakesecretkey"

RUN python3 generate_anomalous_data.py

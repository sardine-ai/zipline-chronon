# FROM python:3.11-slim-buster AS py3
FROM amazoncorretto:17

# COPY --from=py3 / /


 # Install python deps
COPY requirements.txt .
RUN yum install -y python3
RUN  pip3 install --upgrade pip; pip3 install -r requirements.txt

COPY generate_anomalous_data.py . 
COPY start.sh /start.sh
RUN chmod +x /start.sh

ENV DYNAMO_ENDPOINT="http://localhost:8000"
ENV AWS_DEFAULT_REGION="fakeregion"
ENV AWS_ACCESS_KEY_ID="fakeaccesskey"
ENV AWS_SECRET_ACCESS_KEY="fakesecretkey"

ENTRYPOINT ["/start.sh"]
FROM amazoncorretto:17

 # Install python deps
COPY requirements.txt .
RUN yum install -y python3
RUN  pip3 install --upgrade pip; pip3 install -r requirements.txt

RUN mkdir -p /app
COPY generate_anomalous_data.py /app/
COPY start.sh /start.sh
RUN chmod +x /start.sh
WORKDIR /app

ENV DYNAMO_ENDPOINT="http://localhost:8000"
ENV AWS_DEFAULT_REGION="fakeregion"
ENV AWS_ACCESS_KEY_ID="fakeaccesskey"
ENV AWS_SECRET_ACCESS_KEY="fakesecretkey"

ENTRYPOINT ["/start.sh"]
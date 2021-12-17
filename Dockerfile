FROM python:3.8.2-alpine

ADD mqtt_subscriber_client.py /

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

CMD [ "python", "./mqtt_subscriber_client.py" ]
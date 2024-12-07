FROM python:3.9-slim AS python

FROM eclipse-temurin:17.0.7_7-jre

ARG KAFKA_DIR="/opt/"
ARG KAFKA_UNPACKED_FILE="kafka_2.13-3.8.1"
ARG KAFKA_TAR_FILE="kafka_2.13-3.8.1.tgz"
ARG KAFKA_URL="https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz"

RUN apt-get update
RUN mkdir -p $KAFKA_DIR
WORKDIR $KAFKA_DIR

RUN wget $KAFKA_URL
RUN tar -xvf $KAFKA_TAR_FILE
RUN mv $KAFKA_UNPACKED_FILE kafka
RUN rm $KAFKA_TAR_FILE

WORKDIR $KAFKA_DIR/kafka/

RUN apt -y install software-properties-common

COPY images/template.server.properties $KAFKA_DIR/kafka/config/kraft/server.properties
COPY ./images/env_replacer.py $KAFKA_DIR/kafka/config/kraft/env_replacer.py
COPY ./images/run.sh /$KAFKA_DIR/kafka/run.sh
RUN chmod +x /$KAFKA_DIR/kafka/run.sh

CMD ["sh", "-c", "/opt/kafka/run.sh"]
FROM python:3.9-slim AS python

FROM eclipse-temurin:17.0.7_7-jre

ARG KAFKA_DIR="/opt/"
ARG KAFKA_VERSION="kafka_2.13-3.6.0"
ARG KAFKA_FILE="kafka_2.13-3.6.0.tgz"
ARG KAFKA_URL="https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz"

RUN apt-get update
RUN mkdir -p $KAFKA_DIR
WORKDIR $KAFKA_DIR

RUN wget $KAFKA_URL
RUN tar -xvf $KAFKA_FILE
RUN mv $KAFKA_VERSION kafka
RUN rm $KAFKA_FILE

WORKDIR $KAFKA_DIR/kafka/

RUN apt -y install software-properties-common


COPY ./images/server.properties $KAFKA_DIR/kafka/config/kraft/server.properties
COPY ./images/env_replacer.py $KAFKA_DIR/kafka/config/kraft/env_replacer.py

WORKDIR $KAFKA_DIR/kafka/

RUN ./bin/kafka-storage.sh format -t "JFb61d2pD6fe224FbsjoZl" -c config/kraft/server.properties

CMD ["/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/kraft/server.properties"]
TEMPLATE_PROPERTIES=${TEMPLATE_PROPERTIES:-/opt/kafka/config/kraft/server.properties}
echo "Starting Kafka in KRaft mode with properties teplate from $TEMPLATE_PROPERTIES"
python3 /opt/kafka/config/kraft/env_replacer.py "$TEMPLATE_PROPERTIES" /opt/kafka/config/kraft/server.properties
echo "Kafka config:\n"
cat /opt/kafka/config/kraft/server.properties
/opt/kafka/bin/kafka-storage.sh format -t "JFb61d2pD6fe224FbsjoZl" -c config/kraft/server.properties
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
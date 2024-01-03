python3 /opt/kafka/config/kraft/env_replacer.py /opt/kafka/config/kraft/server.properties /opt/kafka/config/kraft/server.properties
echo "Kafka config:\n"
cat /opt/kafka/config/kraft/server.properties
/opt/kafka/bin/kafka-storage.sh format -t "JFb61d2pD6fe224FbsjoZl" -c config/kraft/server.properties
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
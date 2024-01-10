package transactions;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Part1IdempotentProducer {
    private final static Logger logger = LoggerFactory.getLogger(Part1IdempotentProducer.class);
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static String PURCHASE_TOPIC = "purchase";
    private final static String INVOICES_TOPIC = "invoices";
    private final static String SHIPMENTS_TOPIC = "shipments";

    public static void main(String[] args) {
        Properties producerProps = producerProperties();
        Properties consumerProps = consumerProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(PURCHASE_TOPIC));
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        Purchase purchase = mapper.readValue(record.value(), Purchase.class);
                        Invoice invoice = new Invoice(purchase.userId(), purchase.productId(),
                                purchase.quantity(), purchase.totalPrice());
                        Shipment shipment = new Shipment(UUID.randomUUID().toString(), purchase.productId(),
                                purchase.userFullName(), "SomeStreet 12/18, 00-006 Warsaw", purchase.quantity());
                        producer.send(new ProducerRecord<>(INVOICES_TOPIC, mapper.writeValueAsString(invoice))).get();
                        producer.send(new ProducerRecord<>(SHIPMENTS_TOPIC, mapper.writeValueAsString(shipment))).get();
                        consumer.commitSync();
                        logger.info("Successfully processed purchase event: {}", purchase);
                    }
                } catch (Exception ex) {
                    logger.error("Error while processing purchase", ex);
                }
            }
        }
    }

    @NotNull
    private static Properties consumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "idempotenceTest");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return consumerProps;
    }

    @NotNull
    private static Properties producerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // this is set by default when idempotence enabled
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // this enables idempotence
        return props;
    }
}

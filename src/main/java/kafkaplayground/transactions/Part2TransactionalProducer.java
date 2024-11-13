package kafkaplayground.transactions;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import kafkaplayground.ProgramLoop;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Part2TransactionalProducer implements ProgramLoop {
    private final static Logger logger = LoggerFactory.getLogger(Part2TransactionalProducer.class);
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static String PURCHASE_TOPIC = "purchase";
    private final static String INVOICES_TOPIC = "invoices";
    private final static String SHIPMENTS_TOPIC = "shipments";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private volatile boolean running = true;

    public Part2TransactionalProducer() {
        this.consumer = new KafkaConsumer<>(consumerProperties());
        this.producer = new KafkaProducer<>(producerProperties());
    }

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

    private static Properties producerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // this must be set to `all`, if not specified, it will be default for idempotence
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // this enables idempotence
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "part2TransactionalProducer"); // this uniquely identifies the producer, even among restarts
        return props;
    }

    @Override
    public void start() {
        try {
            producer.initTransactions();
            consumer.subscribe(Collections.singleton(PURCHASE_TOPIC));
            while (running) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, String> record : records) {
                        Purchase purchase = mapper.readValue(record.value(), Purchase.class);
                        Invoice invoice = new Invoice(purchase.userId(), purchase.productId(),
                                purchase.quantity(), purchase.totalPrice());
                        Shipment shipment = new Shipment(UUID.randomUUID().toString(), purchase.productId(),
                                purchase.userFullName(), "SomeStreet 12/18, 00-006 Warsaw", purchase.quantity());

                        producer.beginTransaction();
                        producer.send(new ProducerRecord<>(INVOICES_TOPIC, mapper.writeValueAsString(invoice))).get();
                        producer.send(new ProducerRecord<>(SHIPMENTS_TOPIC, mapper.writeValueAsString(shipment))).get();
                        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset()));
                        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                        producer.commitTransaction();
                        logger.info("Successfully processed purchase event: {}", purchase);
                    }
                } catch (Exception ex) {
                    logger.error("Error while processing purchase", ex);
                }
            }
        } finally {
            logger.info("Closing consumer...");
            producer.close();
            consumer.close();
        }
    }

    @Override
    public void wakeup() {
        logger.info("ConsumerLoop wakeup");
        running = false;
    }
}

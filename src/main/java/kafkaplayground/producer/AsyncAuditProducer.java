package kafkaplayground.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import kafkaplayground.ProgramLoop;
import kafkaplayground.producer.AuditLog.ActionType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncAuditProducer implements ProgramLoop {
    private final static Logger logger = LoggerFactory.getLogger(AsyncAuditProducer.class);
    private final static ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger sentCounter = new AtomicInteger(0);
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    private final static String AUDIT_TOPIC = "userActivity";

    private final KafkaProducer<String, String> producer;
    private volatile boolean running = true;

    public AsyncAuditProducer() {
        this.producer = new KafkaProducer<>(producerProperties());
    }

    private static Properties producerProperties() {
        Properties props = new Properties();
        // required parameters
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094,localhost:9095");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // partitioning
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // batching
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100"); // 500ms
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576"); // 1MB

        // sending
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1"); // only leader confirms
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000"); // 10s
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200"); // 200ms
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000"); // 1s
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB
        props.setProperty("partitions.count", "3");

        return props;
    }

    @Override
    public void start() {
        try {
            while (running) {
                try {
                    AuditLog p = generateExampleAuditLog();
                    ProducerRecord<String, String> record = new ProducerRecord<>(AUDIT_TOPIC, mapper.writeValueAsString(p));
                    int i = sequenceNumber.incrementAndGet();
                    record.headers().add("sequenceNumber", String.valueOf(i).getBytes());
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error while sending audit event", exception);
                        } else {
                            int partition = metadata.partition();
                            logger.info("Successfully send audit event with sequence {} to partition {}", sequenceNumber, partition);
                            sentCounter.incrementAndGet();
                        }
                    });
                } catch (Exception ex) {
                    logger.error("Error while sending audit event", ex);
                }
            }
        } finally {
            producer.close();
            logger.info("Closing producer...");
        }
    }

    @Override
    public void wakeup() {
        logger.info("Program loop wakeup");
        running = false;
    }

    private static AuditLog generateExampleAuditLog() {
        ActionType actionType = ActionType.values()[(int) (Math.random() * ActionType.values().length)];
        String username = "user" + (int) (Math.random() * 100);
        String now = Instant.now().toString();
        return new AuditLog(now, username, actionType);
    }
}

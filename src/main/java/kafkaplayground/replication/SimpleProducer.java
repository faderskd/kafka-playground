package kafkaplayground.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import kafkaplayground.ProgramLoop;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleProducer implements ProgramLoop {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static String PURCHASE_TOPIC = "purchase";

    private final KafkaProducer<String, String> producer;
    private volatile boolean running = true;
    private int productId = 0;

    public SimpleProducer() {
        this.producer = new KafkaProducer<>(producerProperties());
    }

    private static Properties producerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG, "true");
        props.setProperty(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, "1000");

        return props;
    }

    @Override
    public void start() {
        try {
            while (running) {
                try {
                    Purchase p = new Purchase(productId++, "Daniel Fąderski", "1$");
                    int partition = producer.send(new ProducerRecord<>(PURCHASE_TOPIC, mapper.writeValueAsString(p))).get().partition();
                    logger.info("Successfully send purchase event to partionon {}", partition);
                } catch (Exception ex) {
                    logger.error("Error while processing purchase", ex);
                }
            }
        } finally {
            producer.close();
            logger.info("Closing producer...");
        }
    }

    @Override
    public void wakeup() {
        logger.info("ConsumerLoop wakeup");
        running = false;
    }
}

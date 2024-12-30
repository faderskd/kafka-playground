package kafkaplayground.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafkaplayground.ProgramLoop;
import kafkaplayground.producer.AuditLog.ActionType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncAuditProducer implements ProgramLoop {
    private final static Logger logger = LoggerFactory.getLogger(AsyncAuditProducer.class);
    private final static Random random = new Random();
    private final static ObjectMapper mapper = new ObjectMapper();

    private final AtomicInteger sentCounter = new AtomicInteger(0);
    private final static String AUDIT_TOPIC = "userActivity";
    private final Timer timer;
    private final KafkaProducer<byte[], byte[]> producer;
    private volatile boolean running = true;
    private long lastReportMillis = System.currentTimeMillis();

    public AsyncAuditProducer() {
        this.producer = new KafkaProducer<>(producerProperties());
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        timer = Timer
                .builder("send.latency")
                .publishPercentiles(0.99)
                .register(meterRegistry);
    }

    private static Properties producerProperties() {
        Properties props = new Properties();
        // required parameters
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9292,localhost:9393,localhost:9494,localhost:9595");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // partitioning
//        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // batching
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "262144"); // 256KB

        // sending
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1"); // only leader confirms
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000"); // 10s
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"); // 5s
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000"); // 1s
//        props.setProperty("partitions.count", "3");

        return props;
    }

    @Override
    public void start() {
        try {
            long startMillis = System.currentTimeMillis();
            while (running) {
                try {
                    AuditLog auditLog = generateExampleAuditLog();

                    ProducerRecord<byte[], byte[]> record =
                            new ProducerRecord<>(AUDIT_TOPIC, mapper.writeValueAsBytes(auditLog));

                    byte [] traceId = "SomeTraceIdFromUpperLayers".getBytes();
                    record.headers().add("trace-id", traceId);

                    long sendTime = System.currentTimeMillis();

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error while sending audit event", exception);
                        } else {
                            timer.record(System.currentTimeMillis() - sendTime, TimeUnit.MILLISECONDS);
                            sentCounter.incrementAndGet();
                            reportMetrics(startMillis);
                        }
                    });
                    if (sentCounter.get() >= 20000000) {
                        break;
                    }
                } catch (Exception ex) {
                    logger.error("Error while sending audit event", ex);
                }
            }
        } finally {
            producer.close();
            logger.info("Closing producer...");
        }
    }

    private void reportMetrics(long startMillis) {
        if (System.currentTimeMillis() - lastReportMillis > 3000) {
            lastReportMillis = System.currentTimeMillis();
            double throughput = 1000 * ((double) sentCounter.get() / (System.currentTimeMillis() - startMillis));
            logger.info("------------------------- Reporting metrics --------------------------------------");
            String throughputMsg = String.format("Send %dK messages. Throughput: %.2f records/s", sentCounter.get() / 1000, throughput);
            logger.info(throughputMsg);
            Arrays.stream(timer.takeSnapshot().percentileValues()).forEach(
                    percentile -> logger.info(
                            "Percentile {} : {}", percentile.percentile(), percentile.value(TimeUnit.MILLISECONDS))
            );
        }
    }

    @Override
    public void wakeup() {
        logger.info("Program loop wakeup");
        running = false;
    }

    private static AuditLog generateExampleAuditLog() {
        ActionType actionType = ActionType.values()[(int) (Math.random() * ActionType.values().length)];
        String userId = "user-" + UUID.randomUUID();
        long timestamp = Instant.now().toEpochMilli();
        return new AuditLog(timestamp, userId, actionType, UUID.randomUUID().toString());
    }

    private static byte[] generatePayload(int length) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            // ASCII A-Z
            payload[i] = (byte) (random.nextInt(26) + 65);
        }
        return payload;
    }
}

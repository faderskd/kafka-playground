package kafkaplayground.producer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPartitioner.class);

    private int partitionsCount;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return counter.getAndIncrement() % partitionsCount;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        partitionsCount = Integer.parseInt((String) configs.get("partitions.count"));
        logger.info("Partitions count: {}", partitionsCount);
    }
}

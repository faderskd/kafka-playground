package kafkaplayground.producer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPartitioner.class);
    private final ConcurrentHashMap<String, AtomicInteger> topicsCounters = new ConcurrentHashMap<>();
    private boolean monitoringEnabled;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        AtomicInteger topicCounter = topicsCounters.computeIfAbsent(topic, (t) -> new AtomicInteger());
        if (monitoringEnabled) {
            // measure something
        }
        return topicCounter.incrementAndGet() % cluster.partitionCountForTopic(topic);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // getting custom properties
        monitoringEnabled = Boolean.parseBoolean((String) configs.get("monitoring.enabled"));
        logger.info("Staring custom round robin partitioner with monitoring enabled: {}", monitoringEnabled);
    }
}

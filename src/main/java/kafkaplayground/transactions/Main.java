package kafkaplayground.transactions;

import kafkaplayground.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        ConsumerLoop consumerLoop = new Part2TransactionalProducer();
        var kafkaConsumerThread = new Thread(consumerLoop::start, "consumer-loop-thread");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Consumer loop wakeup...");
            consumerLoop.wakeup();
            try {
                kafkaConsumerThread.join();
            } catch (InterruptedException e) {
                logger.error("Main thread interrupted", e);
            }
        }, "shutdown-thread"));
        kafkaConsumerThread.start();
    }

}

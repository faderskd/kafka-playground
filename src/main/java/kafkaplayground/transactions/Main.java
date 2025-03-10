package kafkaplayground.transactions;

import kafkaplayground.ProgramLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        ProgramLoop programLoop = new Part2TransactionalProducer();
        var kafkaConsumerThread = new Thread(programLoop::start, "consumer-loop-thread");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Consumer loop wakeup...");
            programLoop.wakeup();
            try {
                kafkaConsumerThread.join();
            } catch (InterruptedException e) {
                logger.error("Main thread interrupted", e);
            }
        }, "shutdown-thread"));
        kafkaConsumerThread.start();
    }

}

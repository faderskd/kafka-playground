package kafkaplayground.producer;

import kafkaplayground.ProgramLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        ProgramLoop programLoop = new AsyncAuditProducer();
        var kafkaProducerThread = new Thread(programLoop::start, "producer-loop-thread");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Producer loop wakeup...");
            programLoop.wakeup();
            try {
                kafkaProducerThread.join();
            } catch (InterruptedException e) {
                logger.error("Main thread interrupted", e);
            }
        }, "shutdown-thread"));
        kafkaProducerThread.start();
    }
}

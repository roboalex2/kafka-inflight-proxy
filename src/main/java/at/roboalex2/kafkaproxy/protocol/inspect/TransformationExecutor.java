package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Shared bounded worker pool for blocking Redis, JCA, record rebuild, and diagnostic JSON work. */
@Component
public class TransformationExecutor {
    private final ThreadPoolExecutor executor;

    @Autowired
    public TransformationExecutor(KafkaProxyProperties properties) {
        this(properties.getProtocol().getTransformationWorkerThreads(),
                properties.getProtocol().getTransformationExecutorQueueCapacity());
    }

    TransformationExecutor(int workerThreads, int queueCapacity) {
        executor = new ThreadPoolExecutor(workerThreads, workerThreads, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                Thread.ofPlatform().name("kafka-transform-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public boolean tryExecute(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            return false;
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) executor.shutdownNow();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}

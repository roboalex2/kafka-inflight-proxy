package at.roboalex2.kafkaproxy.protocol.inspect;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/** Shared virtual-thread executor; each connection supplies its own serial task queue. */
@Component
public class ProtocolInspectionExecutor {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public void execute(Runnable task) {
        executor.execute(task);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}

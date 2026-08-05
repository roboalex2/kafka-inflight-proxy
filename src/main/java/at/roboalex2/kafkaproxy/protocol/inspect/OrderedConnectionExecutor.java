package at.roboalex2.kafkaproxy.protocol.inspect;

import java.util.ArrayDeque;
import java.util.Queue;

class OrderedConnectionExecutor {
    private final ProtocolInspectionExecutor executor;
    private final Queue<Runnable> tasks = new ArrayDeque<>();
    private boolean running;

    OrderedConnectionExecutor(ProtocolInspectionExecutor executor) {
        this.executor = executor;
    }

    synchronized void execute(Runnable task) {
        tasks.add(task);
        if (!running) {
            running = true;
            executor.execute(this::drain);
        }
    }

    private void drain() {
        while (true) {
            Runnable task;
            synchronized (this) {
                task = tasks.poll();
                if (task == null) {
                    running = false;
                    return;
                }
            }
            task.run();
        }
    }
}

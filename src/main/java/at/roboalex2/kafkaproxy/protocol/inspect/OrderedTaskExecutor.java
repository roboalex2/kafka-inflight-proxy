package at.roboalex2.kafkaproxy.protocol.inspect;

import java.util.ArrayDeque;
import java.util.Queue;

class OrderedTaskExecutor {
    private final TransformationExecutor executor;
    private final int capacity;
    private final Queue<Runnable> tasks = new ArrayDeque<>();
    private boolean running;

    OrderedTaskExecutor(TransformationExecutor executor, int capacity) {
        this.executor = executor;
        this.capacity = capacity;
    }

    synchronized boolean execute(Runnable task) {
        if (tasks.size() >= capacity) return false;
        tasks.add(task);
        if (!running) {
            running = true;
            if (!executor.tryExecute(this::drain)) {
                running = false;
                tasks.remove(task);
                return false;
            }
        }
        return true;
    }

    synchronized boolean isAtCapacity() { return tasks.size() >= capacity; }

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

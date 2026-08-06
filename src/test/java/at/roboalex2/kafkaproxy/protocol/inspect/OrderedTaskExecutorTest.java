package at.roboalex2.kafkaproxy.protocol.inspect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class OrderedTaskExecutorTest {
    @Test
    void runsOffCallerInOrderAndRejectsBeyondThePerConnectionBound() throws Exception {
        TransformationExecutor workers = new TransformationExecutor(1, 1);
        try {
            OrderedTaskExecutor ordered = new OrderedTaskExecutor(workers, 1);
            List<Integer> sequence = new CopyOnWriteArrayList<>();
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch releaseFirst = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(2);
            String callerThread = Thread.currentThread().getName();
            List<String> workerThreads = new CopyOnWriteArrayList<>();

            assertThat(ordered.execute(() -> {
                workerThreads.add(Thread.currentThread().getName());
                sequence.add(1);
                firstStarted.countDown();
                try { releaseFirst.await(); } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
                finished.countDown();
            })).isTrue();
            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(ordered.execute(() -> {
                workerThreads.add(Thread.currentThread().getName());
                sequence.add(2);
                finished.countDown();
            })).isTrue();
            assertThat(ordered.isAtCapacity()).isTrue();
            assertThat(ordered.execute(() -> sequence.add(3))).isFalse();

            releaseFirst.countDown();
            assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(sequence).containsExactly(1, 2);
            assertThat(workerThreads).allMatch(name -> !name.equals(callerThread));
        } finally {
            workers.shutdown();
        }
    }
}

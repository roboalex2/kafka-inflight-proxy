package at.roboalex2.kafkaproxy.logging;

public interface ConnectionLogWriter extends AutoCloseable {
    boolean isEnabled();
    void append(String entry);
    @Override void close();
}

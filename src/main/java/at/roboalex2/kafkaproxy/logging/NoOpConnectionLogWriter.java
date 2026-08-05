package at.roboalex2.kafkaproxy.logging;

public class NoOpConnectionLogWriter implements ConnectionLogWriter {
    @Override public boolean isEnabled() { return false; }
    @Override public void append(String entry) { }
    @Override public void close() { }
}

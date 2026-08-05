package at.roboalex2.kafkaproxy.network;
/** Owns one client channel, one broker channel, and their shared connection lifecycle. */
public interface ConnectionPair extends AutoCloseable { @Override void close(); }

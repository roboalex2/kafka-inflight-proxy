package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;

/** Owns one client channel, one broker channel, and their shared connection lifecycle. */
public interface ConnectionPair extends AutoCloseable {
    String getId();

    Channel getClientChannel();

    Channel getBrokerChannel();

    boolean isClosed();

    @Override
    void close();
}

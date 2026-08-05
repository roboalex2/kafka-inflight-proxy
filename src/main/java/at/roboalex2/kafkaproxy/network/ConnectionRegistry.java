package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;

/** Tracks active connection pairs so application shutdown can close them cleanly. */
public interface ConnectionRegistry {
    void startAccepting();

    boolean registerPending(Channel clientChannel);

    void unregisterPending(Channel clientChannel);

    boolean register(ConnectionPair connectionPair);

    void unregister(ConnectionPair connectionPair);

    void closeAll();

    int size();
}

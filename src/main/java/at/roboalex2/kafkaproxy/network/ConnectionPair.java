package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.protocol.inspect.ConnectionProtocolContext;
import io.netty.channel.Channel;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns one client channel, one broker channel, and their shared connection lifecycle. */
public class ConnectionPair implements AutoCloseable {
    private final Channel clientChannel;
    private final Channel brokerChannel;
    private final ConnectionRegistry connectionRegistry;
    private final ConnectionProtocolContext protocolContext;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ConnectionPair(Channel clientChannel, Channel brokerChannel,
                          ConnectionRegistry connectionRegistry) {
        this(clientChannel, brokerChannel, connectionRegistry, null);
    }

    public ConnectionPair(Channel clientChannel, Channel brokerChannel,
                          ConnectionRegistry connectionRegistry,
                          ConnectionProtocolContext protocolContext) {
        this.clientChannel = clientChannel;
        this.brokerChannel = brokerChannel;
        this.connectionRegistry = connectionRegistry;
        this.protocolContext = protocolContext;
    }

    public void activateCloseCoupling() {
        clientChannel.closeFuture().addListener(ignored -> close());
        brokerChannel.closeFuture().addListener(ignored -> close());
    }

    public String getId() { return clientChannel.id().asLongText(); }
    public Channel getClientChannel() { return clientChannel; }
    public Channel getBrokerChannel() { return brokerChannel; }
    public boolean isClosed() { return closed.get(); }
    public boolean isTransformationQueueAtCapacity() {
        return protocolContext != null && protocolContext.isTransformationQueueAtCapacity();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        connectionRegistry.unregister(this);
        if (protocolContext != null) protocolContext.close();
        closeIfNeeded(clientChannel);
        closeIfNeeded(brokerChannel);
    }

    private void closeIfNeeded(Channel channel) {
        if (channel.isOpen()) channel.close();
    }
}

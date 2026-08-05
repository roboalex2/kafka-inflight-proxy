package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultConnectionPair implements ConnectionPair {
    private final Channel clientChannel;
    private final Channel brokerChannel;
    private final ConnectionRegistry connectionRegistry;
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultConnectionPair(Channel clientChannel, Channel brokerChannel,
                                 ConnectionRegistry connectionRegistry) {
        this.clientChannel = clientChannel;
        this.brokerChannel = brokerChannel;
        this.connectionRegistry = connectionRegistry;
    }

    public void activateCloseCoupling() {
        clientChannel.closeFuture().addListener(ignored -> close());
        brokerChannel.closeFuture().addListener(ignored -> close());
    }

    @Override
    public String getId() {
        return clientChannel.id().asLongText();
    }

    @Override
    public Channel getClientChannel() {
        return clientChannel;
    }

    @Override
    public Channel getBrokerChannel() {
        return brokerChannel;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        connectionRegistry.unregister(this);
        closeIfNeeded(clientChannel);
        closeIfNeeded(brokerChannel);
    }

    private void closeIfNeeded(Channel channel) {
        if (channel.isOpen()) {
            channel.close();
        }
    }
}

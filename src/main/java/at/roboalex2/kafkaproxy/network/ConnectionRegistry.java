package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

/** Tracks pending clients and active connection pairs for coordinated shutdown. */
@Component
public class ConnectionRegistry {
    private final ConcurrentMap<String, ConnectionPair> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Channel> pendingClients = new ConcurrentHashMap<>();
    private final AtomicBoolean accepting = new AtomicBoolean();

    public void startAccepting() { accepting.set(true); }

    public boolean registerPending(Channel clientChannel) {
        if (!accepting.get()) {
            clientChannel.close();
            return false;
        }
        pendingClients.put(clientChannel.id().asLongText(), clientChannel);
        if (!accepting.get() && pendingClients.remove(clientChannel.id().asLongText(), clientChannel)) {
            clientChannel.close();
            return false;
        }
        return true;
    }

    public void unregisterPending(Channel clientChannel) {
        pendingClients.remove(clientChannel.id().asLongText(), clientChannel);
    }

    public boolean register(ConnectionPair connectionPair) {
        if (!accepting.get()) return false;
        connections.put(connectionPair.getId(), connectionPair);
        if (!accepting.get()) {
            connections.remove(connectionPair.getId(), connectionPair);
            return false;
        }
        return true;
    }

    public void unregister(ConnectionPair connectionPair) {
        connections.remove(connectionPair.getId(), connectionPair);
    }

    public void closeAll() {
        accepting.set(false);
        List.copyOf(pendingClients.values()).forEach(Channel::close);
        pendingClients.clear();
        List.copyOf(connections.values()).forEach(ConnectionPair::close);
    }

    public int size() { return connections.size(); }
}

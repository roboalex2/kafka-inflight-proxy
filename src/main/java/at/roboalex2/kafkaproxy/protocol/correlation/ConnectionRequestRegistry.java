package at.roboalex2.kafkaproxy.protocol.correlation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Correlation state owned by exactly one client/broker connection pair. */
public class ConnectionRequestRegistry {
    private final ConcurrentMap<Integer, RequestContext> pendingRequests = new ConcurrentHashMap<>();

    public boolean register(RequestContext requestContext) {
        if (!requestContext.isExpectsResponse()) {
            return true;
        }
        return pendingRequests.putIfAbsent(requestContext.getCorrelationId(), requestContext) == null;
    }

    public RequestContext remove(int correlationId) {
        return pendingRequests.remove(correlationId);
    }

    public int size() {
        return pendingRequests.size();
    }

    public void clear() {
        pendingRequests.clear();
    }
}

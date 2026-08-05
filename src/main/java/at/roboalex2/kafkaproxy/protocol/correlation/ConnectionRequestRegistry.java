package at.roboalex2.kafkaproxy.protocol.correlation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Correlation state owned by exactly one client/broker connection pair. */
public class ConnectionRequestRegistry implements RequestCorrelationTracker {
    private final ConcurrentMap<Integer, RequestContext> pendingRequests = new ConcurrentHashMap<>();

    @Override
    public boolean register(RequestContext requestContext) {
        if (!requestContext.isExpectsResponse()) {
            return true;
        }
        return pendingRequests.putIfAbsent(requestContext.getCorrelationId(), requestContext) == null;
    }

    @Override
    public RequestContext remove(int correlationId) {
        return pendingRequests.remove(correlationId);
    }

    @Override
    public int size() {
        return pendingRequests.size();
    }

    @Override
    public void clear() {
        pendingRequests.clear();
    }
}

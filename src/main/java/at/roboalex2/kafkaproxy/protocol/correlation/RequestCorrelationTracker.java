package at.roboalex2.kafkaproxy.protocol.correlation;

public interface RequestCorrelationTracker {
    boolean register(RequestContext requestContext);
    RequestContext remove(int correlationId);
    int size();
    void clear();
}

package at.roboalex2.kafkaproxy.protocol.correlation;

import java.time.Instant;

public class RequestContext {
    private final String connectionId;
    private final int correlationId;
    private final short apiKey;
    private final String apiName;
    private final short apiVersion;
    private final short requestHeaderVersion;
    private final short responseHeaderVersion;
    private final boolean expectsResponse;
    private final long requestSequenceNumber;
    private final Instant receivedAt;

    public RequestContext(String connectionId, int correlationId, short apiKey, String apiName,
                          short apiVersion, short requestHeaderVersion, short responseHeaderVersion,
                          boolean expectsResponse, long requestSequenceNumber, Instant receivedAt) {
        this.connectionId = connectionId;
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.apiName = apiName;
        this.apiVersion = apiVersion;
        this.requestHeaderVersion = requestHeaderVersion;
        this.responseHeaderVersion = responseHeaderVersion;
        this.expectsResponse = expectsResponse;
        this.requestSequenceNumber = requestSequenceNumber;
        this.receivedAt = receivedAt;
    }

    public String getConnectionId() { return connectionId; }
    public int getCorrelationId() { return correlationId; }
    public short getApiKey() { return apiKey; }
    public String getApiName() { return apiName; }
    public short getApiVersion() { return apiVersion; }
    public short getRequestHeaderVersion() { return requestHeaderVersion; }
    public short getResponseHeaderVersion() { return responseHeaderVersion; }
    public boolean isExpectsResponse() { return expectsResponse; }
    public long getRequestSequenceNumber() { return requestSequenceNumber; }
    public Instant getReceivedAt() { return receivedAt; }
}

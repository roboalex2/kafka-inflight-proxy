package at.roboalex2.kafkaproxy.protocol.codec;

import at.roboalex2.kafkaproxy.protocol.model.ProtocolMessageModel;

public class ParsedProtocolMessage {
    private final int correlationId;
    private final short apiKey;
    private final String apiName;
    private final short apiVersion;
    private final short headerVersion;
    private final boolean supportedBody;
    private final boolean expectsResponse;
    private final ProtocolMessageModel model;

    public ParsedProtocolMessage(int correlationId, short apiKey, String apiName, short apiVersion,
                                 short headerVersion, boolean supportedBody, boolean expectsResponse,
                                 ProtocolMessageModel model) {
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.apiName = apiName;
        this.apiVersion = apiVersion;
        this.headerVersion = headerVersion;
        this.supportedBody = supportedBody;
        this.expectsResponse = expectsResponse;
        this.model = model;
    }

    public int getCorrelationId() { return correlationId; }
    public short getApiKey() { return apiKey; }
    public String getApiName() { return apiName; }
    public short getApiVersion() { return apiVersion; }
    public short getHeaderVersion() { return headerVersion; }
    public boolean isSupportedBody() { return supportedBody; }
    public boolean isExpectsResponse() { return expectsResponse; }
    public ProtocolMessageModel getModel() { return model; }
}

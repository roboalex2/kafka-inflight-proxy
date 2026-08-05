package at.roboalex2.kafkaproxy.protocol.codec;

import at.roboalex2.kafkaproxy.protocol.model.ProtocolMessageModel;
import org.apache.kafka.common.protocol.ApiMessage;

public class ParsedProtocolMessage {
    private final int correlationId;
    private final short apiKey;
    private final String apiName;
    private final short apiVersion;
    private final short headerVersion;
    private final boolean supportedBody;
    private final boolean expectsResponse;
    private final ProtocolMessageModel model;
    private final ApiMessage headerData;
    private final ApiMessage messageData;

    public ParsedProtocolMessage(int correlationId, short apiKey, String apiName, short apiVersion,
                                 short headerVersion, boolean supportedBody, boolean expectsResponse,
                                 ProtocolMessageModel model) {
        this(correlationId, apiKey, apiName, apiVersion, headerVersion, supportedBody,
                expectsResponse, model, null, null);
    }

    public ParsedProtocolMessage(int correlationId, short apiKey, String apiName, short apiVersion,
                                 short headerVersion, boolean supportedBody, boolean expectsResponse,
                                 ProtocolMessageModel model, ApiMessage headerData, ApiMessage messageData) {
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.apiName = apiName;
        this.apiVersion = apiVersion;
        this.headerVersion = headerVersion;
        this.supportedBody = supportedBody;
        this.expectsResponse = expectsResponse;
        this.model = model;
        this.headerData = headerData;
        this.messageData = messageData;
    }

    public int getCorrelationId() { return correlationId; }
    public short getApiKey() { return apiKey; }
    public String getApiName() { return apiName; }
    public short getApiVersion() { return apiVersion; }
    public short getHeaderVersion() { return headerVersion; }
    public boolean isSupportedBody() { return supportedBody; }
    public boolean isExpectsResponse() { return expectsResponse; }
    public ProtocolMessageModel getModel() { return model; }
    public ApiMessage getHeaderData() { return headerData; }
    public ApiMessage getMessageData() { return messageData; }
}

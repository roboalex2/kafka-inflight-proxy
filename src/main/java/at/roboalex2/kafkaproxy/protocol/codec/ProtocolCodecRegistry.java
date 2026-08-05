package at.roboalex2.kafkaproxy.protocol.codec;

import at.roboalex2.kafkaproxy.protocol.correlation.RequestContext;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.model.ProtocolMessageModel;
import java.nio.ByteBuffer;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.springframework.stereotype.Component;

/** Version-aware header and body codec backed by Kafka's generated schemas. */
@Component
public class ProtocolCodecRegistry {
    private final ProtocolModelMapper modelMapper;

    public ProtocolCodecRegistry(ProtocolModelMapper modelMapper) {
        this.modelMapper = modelMapper;
    }

    public ParsedProtocolMessage parseRequest(ByteBuffer body) {
        if (body.remaining() < 8) {
            throw new IllegalArgumentException("Kafka request header is shorter than 8 bytes");
        }
        short rawApiKey = body.getShort(body.position());
        short version = body.getShort(body.position() + 2);
        int correlationId = body.getInt(body.position() + 4);
        if (!ApiKeys.hasId(rawApiKey)) {
            return new ParsedProtocolMessage(correlationId, rawApiKey, "Unknown", version,
                    (short) -1, false, false, null);
        }

        ApiKeys api = ApiKeys.forId(rawApiKey);
        if (!api.isVersionSupported(version)) {
            return new ParsedProtocolMessage(correlationId, rawApiKey, api.name, version,
                    safeRequestHeaderVersion(api, version), false, true, null);
        }

        RequestHeader header = RequestHeader.parse(body);
        boolean detailed = supportsDetailedBody(api, version);
        ProtocolMessageModel model = null;
        boolean expectsResponse = true;
        if (detailed) {
            ApiMessage data = api.messageType.newRequest();
            data.read(new ByteBufferAccessor(body), version);
            model = modelMapper.mapRequest(rawApiKey, version, data);
            if (data instanceof ProduceRequestData produceRequest) {
                expectsResponse = produceRequest.acks() != 0;
            }
        }
        return new ParsedProtocolMessage(header.correlationId(), rawApiKey, api.name, version,
                header.headerVersion(), detailed, expectsResponse, model);
    }

    public ParsedProtocolMessage parseResponse(ByteBuffer body, RequestContext request) {
        ResponseHeader header = ResponseHeader.parse(body, request.getResponseHeaderVersion());
        ApiKeys api = ApiKeys.forId(request.getApiKey());
        boolean detailed = supportsDetailedBody(api, request.getApiVersion());
        ProtocolMessageModel model = null;
        if (detailed) {
            ApiMessage data = api.messageType.newResponse();
            data.read(new ByteBufferAccessor(body), request.getApiVersion());
            model = modelMapper.mapResponse(request.getApiKey(), request.getApiVersion(), data);
        }
        return new ParsedProtocolMessage(header.correlationId(), request.getApiKey(), request.getApiName(),
                request.getApiVersion(), header.headerVersion(), detailed, true, model);
    }

    public short responseHeaderVersion(short apiKey, short version) {
        if (!ApiKeys.hasId(apiKey)) {
            return 0;
        }
        try {
            return ApiKeys.forId(apiKey).responseHeaderVersion(version);
        } catch (RuntimeException ignored) {
            return 0;
        }
    }

    private short safeRequestHeaderVersion(ApiKeys api, short version) {
        try {
            return api.requestHeaderVersion(version);
        } catch (RuntimeException ignored) {
            return -1;
        }
    }

    private boolean supportsDetailedBody(ApiKeys api, short version) {
        return switch (api) {
            case METADATA -> version >= 0 && version <= 13;
            case PRODUCE -> version >= 3 && version <= 13;
            case FETCH -> version >= 4 && version <= 18;
            default -> false;
        };
    }
}

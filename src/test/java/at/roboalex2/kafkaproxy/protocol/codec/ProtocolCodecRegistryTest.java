package at.roboalex2.kafkaproxy.protocol.codec;

import static org.assertj.core.api.Assertions.assertThat;

import at.roboalex2.kafkaproxy.protocol.correlation.RequestContext;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.stream.IntStream;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.Test;

class ProtocolCodecRegistryTest {
    private final ProtocolCodecRegistry codecs = new ProtocolCodecRegistry(new ProtocolModelMapper());

    @Test
    void parsesEveryRequiredRequestAndResponseVersion() {
        assertVersionRange(ApiKeys.METADATA, 0, 13);
        assertVersionRange(ApiKeys.PRODUCE, 3, 13);
        assertVersionRange(ApiKeys.FETCH, 4, 18);
    }

    @Test
    void identifiesUnknownApiWithoutGuessingItsSchema() {
        ByteBuffer unknown = ByteBuffer.allocate(8).putShort((short) 120).putShort((short) 1).putInt(55).flip();
        ParsedProtocolMessage parsed = codecs.parseRequest(unknown);
        assertThat(parsed.getApiName()).isEqualTo("Unknown");
        assertThat(parsed.getCorrelationId()).isEqualTo(55);
        assertThat(parsed.getModel()).isNull();
    }

    @Test
    void produceAcksZeroDoesNotExpectAResponse() {
        short version = 13;
        ProduceRequestData noResponse = new ProduceRequestData().setAcks((short) 0);
        ProduceRequestData withResponse = new ProduceRequestData().setAcks((short) 1);

        assertThat(codecs.parseRequest(requestBytes(ApiKeys.PRODUCE, version, 1, noResponse))
                .isExpectsResponse()).isFalse();
        assertThat(codecs.parseRequest(requestBytes(ApiKeys.PRODUCE, version, 2, withResponse))
                .isExpectsResponse()).isTrue();
    }

    private void assertVersionRange(ApiKeys api, int first, int last) {
        IntStream.rangeClosed(first, last).forEach(rawVersion -> {
            short version = (short) rawVersion;
            ParsedProtocolMessage request = codecs.parseRequest(requestBytes(api, version, 91));
            assertThat(request.getApiName()).isEqualTo(api.name);
            assertThat(request.getApiVersion()).isEqualTo(version);
            assertThat(request.getModel()).isNotNull();

            RequestContext context = new RequestContext("connection", 91, api.id, api.name, version,
                    api.requestHeaderVersion(version), api.responseHeaderVersion(version), true, 1, Instant.EPOCH);
            ParsedProtocolMessage response = codecs.parseResponse(responseBytes(api, version, 91), context);
            assertThat(response.getCorrelationId()).isEqualTo(91);
            assertThat(response.getModel()).isNotNull();
        });
    }

    private ByteBuffer requestBytes(ApiKeys api, short version, int correlationId) {
        return requestBytes(api, version, correlationId, api.messageType.newRequest());
    }

    private ByteBuffer requestBytes(ApiKeys api, short version, int correlationId, ApiMessage message) {
        RequestHeader header = new RequestHeader(api, version, "fixture", correlationId);
        return concatenate(serialized(header.data(), header.headerVersion()),
                serialized(message, version));
    }

    private ByteBuffer responseBytes(ApiKeys api, short version, int correlationId) {
        ResponseHeader header = new ResponseHeader(correlationId, api.responseHeaderVersion(version));
        return concatenate(serialized(header.data(), header.headerVersion()),
                serialized(api.messageType.newResponse(), version));
    }

    private ByteBuffer serialized(ApiMessage message, short version) {
        return MessageUtil.toByteBufferAccessor(message, version).buffer();
    }

    private ByteBuffer concatenate(ByteBuffer first, ByteBuffer second) {
        ByteBuffer result = ByteBuffer.allocate(first.remaining() + second.remaining());
        result.put(first.duplicate()).put(second.duplicate()).flip();
        return result;
    }
}

package at.roboalex2.kafkaproxy.protocol.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class KafkaProtocolMessageSerializerTest {
    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
    void producesClientParseableMetadataForEverySupportedVersion(short version) {
        short headerVersion = ApiKeys.METADATA.responseHeaderVersion(version);
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(41);
        MetadataResponseData response = new MetadataResponseData();
        response.brokers().add(new MetadataResponseBroker().setNodeId(7)
                .setHost("proxy-host").setPort(19092));

        ByteBuffer serialized = new KafkaProtocolMessageSerializer()
                .serializeResponse(header, headerVersion, response, version);
        assertThat(serialized.getInt()).isEqualTo(serialized.remaining());
        ResponseHeaderData parsedHeader = new ResponseHeaderData(new ByteBufferAccessor(serialized), headerVersion);
        MetadataResponseData parsed = new MetadataResponseData(new ByteBufferAccessor(serialized), version);

        assertThat(parsedHeader.correlationId()).isEqualTo(41);
        assertThat(parsed.brokers().find(7).host()).isEqualTo("proxy-host");
        assertThat(parsed.brokers().find(7).port()).isEqualTo(19092);
        assertThat(serialized.remaining()).isZero();
    }

    @Test
    void writesCorrectLengthAndPreservesFlexibleHeaderAndBodyTags() {
        short version = 13;
        short headerVersion = 1;
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(73);
        header.unknownTaggedFields().add(new RawTaggedField(9, new byte[]{1, 2}));
        MetadataResponseData response = new MetadataResponseData().setClusterId("cluster");
        response.unknownTaggedFields().add(new RawTaggedField(12, new byte[]{3, 4, 5}));

        ByteBuffer serialized = new KafkaProtocolMessageSerializer()
                .serializeResponse(header, headerVersion, response, version);

        assertThat(serialized.getInt()).isEqualTo(serialized.remaining());
        ResponseHeaderData parsedHeader = new ResponseHeaderData(new ByteBufferAccessor(serialized), headerVersion);
        MetadataResponseData parsedBody = new MetadataResponseData(new ByteBufferAccessor(serialized), version);
        assertThat(parsedHeader.correlationId()).isEqualTo(73);
        assertThat(parsedHeader.unknownTaggedFields()).isEqualTo(header.unknownTaggedFields());
        assertThat(parsedBody).isEqualTo(response);
        assertThat(serialized.remaining()).isZero();
    }
}

package at.roboalex2.kafkaproxy.protocol.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import at.roboalex2.kafkaproxy.protocol.model.RecordBatchModel;
import at.roboalex2.kafkaproxy.protocol.model.ProtocolMessageModel;
import at.roboalex2.kafkaproxy.protocol.model.TaggedField;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ProtocolModelMapperTest {
    private final ProtocolModelMapper mapper = new ProtocolModelMapper();

    @Test
    void preservesOpaqueTaggedFieldsAsTypedModels() {
        MetadataRequestData data = new MetadataRequestData();
        data.unknownTaggedFields().add(new RawTaggedField(27, new byte[]{4, 5, 6}));

        ProtocolMessageModel model = mapper.mapRequest((short) 3, (short) 13, data);

        Object rawTags = model.getFields().get("unknownTaggedFields");
        assertThat(rawTags).isInstanceOf(List.class);
        assertThat((List<?>) rawTags).singleElement().isInstanceOf(TaggedField.class);
        TaggedField taggedField = (TaggedField) ((List<?>) rawTags).getFirst();
        assertThat(taggedField.getTag()).isEqualTo(27);
        assertThat(taggedField.getValue()).containsExactly(4, 5, 6);
    }

    @Test
    void detailedModelsSerializeAsPrettyJson() throws Exception {
        ProtocolMessageModel model = mapper.mapRequest((short) 3, (short) 13, new MetadataRequestData());
        String json = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(model);
        assertThat(json).contains(System.lineSeparator(), "\"apiVersion\" : 13", "\"fields\"");
    }

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void mapsRecordsAndHeadersThroughEveryRequiredCompressionCodec(CompressionType compressionType) {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        byte[] headerValue = {1, 2, 3};
        SimpleRecord record = new SimpleRecord(1_234L, key, value,
                new Header[]{new RecordHeader("trace", headerValue)});
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.of(compressionType).build(), record);

        List<RecordBatchModel> batches = mapper.mapRecords(records);

        assertThat(batches).hasSize(1);
        RecordBatchModel batch = batches.getFirst();
        assertThat(batch.getCompressionType()).isEqualTo(compressionType.name());
        assertThat(batch.getRecords()).hasSize(1);
        assertThat(batch.getRecords().getFirst().getKey()).isEqualTo(key);
        assertThat(batch.getRecords().getFirst().getValue()).isEqualTo(value);
        assertThat(batch.getRecords().getFirst().getHeaders().getFirst().getKey()).isEqualTo("trace");
        assertThat(batch.getRecords().getFirst().getHeaders().getFirst().getValue()).isEqualTo(headerValue);
    }

    static Stream<CompressionType> compressionTypes() {
        return Stream.of(CompressionType.NONE, CompressionType.GZIP, CompressionType.SNAPPY,
                CompressionType.LZ4, CompressionType.ZSTD);
    }
}

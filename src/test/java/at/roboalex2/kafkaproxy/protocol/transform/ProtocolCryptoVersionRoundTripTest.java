package at.roboalex2.kafkaproxy.protocol.transform;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProtocolCryptoVersionRoundTripTest {
    private static final UUID TOPIC_ID = UUID.fromString("50000000-0000-0000-0000-000000000005");
    private static final Uuid KAFKA_TOPIC_ID = new Uuid(TOPIC_ID.getMostSignificantBits(),
            TOPIC_ID.getLeastSignificantBits());

    @ParameterizedTest
    @ValueSource(shorts = {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
    void produceVersionsEncryptAndRoundTrip(short version) {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        fixture.topicIdentityResolver.observe("orders", KAFKA_TOPIC_ID);
        ProduceRequestData parsed = parseProduce(serialize(produce(plainRecords()), version), version);
        ProduceRequestData encrypted = (ProduceRequestData) fixture.produceTransformer.transform(parsed).message();
        ProduceRequestData reparsed = parseProduce(serialize(encrypted, version), version);
        MemoryRecords records = (MemoryRecords) reparsed.topicData().iterator().next()
                .partitionData().getFirst().records();
        Record record = records(records).getFirst();
        assertThat(copy(record.key())).isNotEqualTo("key".getBytes(StandardCharsets.UTF_8));
        assertThat(record.headers()).extracting(org.apache.kafka.common.header.Header::key)
                .contains("encryption-key", "encryption-iv");
        records.batches().iterator().next().ensureValid();
    }

    @ParameterizedTest
    @ValueSource(shorts = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18})
    void fetchVersionsDecryptAndRoundTrip(short version) {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        fixture.topicIdentityResolver.observe("orders", KAFKA_TOPIC_ID);
        MemoryRecords encrypted = (MemoryRecords) ((ProduceRequestData) fixture.produceTransformer
                .transform(produce(plainRecords())).message()).topicData().iterator().next()
                .partitionData().getFirst().records();
        FetchResponseData parsed = parseFetch(serialize(fetch(encrypted), version), version);
        FetchResponseData decrypted = (FetchResponseData) fixture.fetchTransformer
                .transform("version-test", parsed).message();
        FetchResponseData reparsed = parseFetch(serialize(decrypted, version), version);
        MemoryRecords records = (MemoryRecords) reparsed.responses().getFirst().partitions().getFirst().records();
        Record record = records(records).getFirst();
        assertThat(copy(record.key())).isEqualTo("key".getBytes(StandardCharsets.UTF_8));
        assertThat(copy(record.value())).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
        records.batches().iterator().next().ensureValid();
    }

    private ProduceRequestData produce(MemoryRecords records) {
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData()
                .setName("orders").setTopicId(KAFKA_TOPIC_ID)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0).setRecords(records)));
        return new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1_000)
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(List.of(topic).iterator()));
    }

    private FetchResponseData fetch(MemoryRecords records) {
        return new FetchResponseData().setResponses(List.of(new FetchResponseData.FetchableTopicResponse()
                .setTopic("orders").setTopicId(KAFKA_TOPIC_ID)
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(0).setRecords(records)))));
    }

    private MemoryRecords plainRecords() {
        return MemoryRecords.withRecords(Compression.of(CompressionType.NONE).build(),
                new SimpleRecord("key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8)));
    }

    private ByteBuffer serialize(org.apache.kafka.common.protocol.ApiMessage message, short version) {
        return MessageUtil.toByteBufferAccessor(message, version).buffer();
    }

    private ProduceRequestData parseProduce(ByteBuffer bytes, short version) {
        return new ProduceRequestData(new ByteBufferAccessor(bytes.duplicate()), version);
    }

    private FetchResponseData parseFetch(ByteBuffer bytes, short version) {
        return new FetchResponseData(new ByteBufferAccessor(bytes.duplicate()), version);
    }

    private List<Record> records(MemoryRecords records) {
        List<Record> result = new ArrayList<>();
        records.records().forEach(result::add);
        return result;
    }

    private byte[] copy(ByteBuffer value) {
        ByteBuffer duplicate = value.duplicate();
        byte[] copy = new byte[duplicate.remaining()];
        duplicate.get(copy);
        return copy;
    }
}

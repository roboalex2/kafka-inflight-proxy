package at.roboalex2.kafkaproxy.protocol.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.api.error.BackendException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.ControlRecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ProduceFetchCryptographicTransformerTest {
    private static final String KEY_HEADER = "encryption-key";
    private static final String IV_HEADER = "encryption-iv";
    private static final UUID TOPIC_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final String TOPIC = "orders";

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void encryptsAndDecryptsEveryCompressionTypeWhilePreservingBatchAndNullSemantics(
            CompressionType compressionType) {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        MemoryRecords plaintext = records(compressionType,
                new SimpleRecord(1_000L, (byte[]) null, (byte[]) null,
                        new Header[]{new RecordHeader("nullable", null)}),
                new SimpleRecord(1_001L, new byte[0], new byte[0],
                        new Header[]{new RecordHeader("first", new byte[0]),
                                new RecordHeader("first", "value".getBytes(StandardCharsets.UTF_8))}));
        DefaultRecordBatch originalBatch = batch(plaintext);
        ProduceRequestData produce = produce(TOPIC_ID, TOPIC, plaintext);

        MessageTransformationResult encryptedResult = fixture.produceTransformer.transform(produce);
        ProduceRequestData encrypted = (ProduceRequestData) encryptedResult.message();
        MemoryRecords encryptedRecords = recordsOf(encrypted);
        DefaultRecordBatch encryptedBatch = batch(encryptedRecords);
        List<Record> encryptedValues = recordList(encryptedRecords);

        assertThat(encryptedResult.changed()).isTrue();
        assertThat(encryptedBatch.compressionType()).isEqualTo(compressionType);
        assertBatchMetadata(encryptedBatch, originalBatch);
        encryptedBatch.ensureValid();
        assertThat(encryptedValues).hasSize(2);
        assertThat(encryptedValues.get(0).hasKey()).isFalse();
        assertThat(encryptedValues.get(0).hasValue()).isFalse();
        assertThat(encryptedValues.get(1).keySize()).isEqualTo(16);
        assertThat(encryptedValues.get(1).valueSize()).isEqualTo(16);
        assertThat(header(encryptedValues.get(1), KEY_HEADER).value()).isNotEmpty();
        assertThat(Base64.getUrlDecoder().decode(new String(header(encryptedValues.get(1), IV_HEADER).value(),
                StandardCharsets.UTF_8))).hasSize(12);
        assertThat(encryptedValues.get(1).headers()[0].key()).startsWith("enc:");

        FetchResponseData fetch = fetch(TOPIC_ID, TOPIC, encryptedRecords);
        MessageTransformationResult decryptedResult = fixture.fetchTransformer.transform("connection-test", fetch);
        MemoryRecords decryptedRecords = recordsOf((FetchResponseData) decryptedResult.message());
        DefaultRecordBatch decryptedBatch = batch(decryptedRecords);
        List<Record> decrypted = recordList(decryptedRecords);

        assertThat(decryptedResult.changed()).isTrue();
        assertThat(decryptedBatch.compressionType()).isEqualTo(compressionType);
        assertBatchMetadata(decryptedBatch, originalBatch);
        decryptedBatch.ensureValid();
        assertThat(decrypted.get(0).hasKey()).isFalse();
        assertThat(decrypted.get(0).hasValue()).isFalse();
        assertThat(decrypted.get(0).headers()).extracting(Header::key)
                .containsExactly("nullable", KEY_HEADER, IV_HEADER);
        assertThat(decrypted.get(0).headers()[0].value()).isNull();
        assertThat(copy(decrypted.get(1).key())).isEmpty();
        assertThat(copy(decrypted.get(1).value())).isEmpty();
        assertThat(decrypted.get(1).headers()).extracting(Header::key)
                .containsExactly("first", "first", KEY_HEADER, IV_HEADER);
        assertThat(decrypted.get(1).headers()[0].value()).isEmpty();
        assertThat(decrypted.get(1).headers()[1].value()).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void reusesAssignmentForSameTopicAndOriginalKeyButUsesFreshRecordNonce() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        byte[] key = "customer-42".getBytes(StandardCharsets.UTF_8);
        ProduceRequestData produce = produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord(key, "one".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(key, "two".getBytes(StandardCharsets.UTF_8))));

        MemoryRecords encrypted = recordsOf((ProduceRequestData) fixture.produceTransformer.transform(produce).message());
        List<Record> records = recordList(encrypted);
        assertThat(new String(header(records.get(0), KEY_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo(new String(header(records.get(1), KEY_HEADER).value(), StandardCharsets.UTF_8));
        assertThat(header(records.get(0), IV_HEADER).value()).isNotEqualTo(header(records.get(1), IV_HEADER).value());
        assertThat(fixture.assignmentRepository.assignments).hasSize(1);
    }

    @Test
    void anotherTopicOrKeyReceivesAnotherAssignment() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        fixture.produceTransformer.transform(produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8), null))));
        fixture.produceTransformer.transform(produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord("b".getBytes(StandardCharsets.UTF_8), null))));
        fixture.produceTransformer.transform(produce(UUID.randomUUID(), "other", records(CompressionType.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8), null))));
        assertThat(fixture.assignmentRepository.assignments.values()).hasSize(3).doesNotHaveDuplicates();
    }

    @Test
    void deletedKeyAndTamperedRecordsBecomeIndependentTombstones() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        ProduceRequestData produce = produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8), "one".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("second".getBytes(StandardCharsets.UTF_8), "two".getBytes(StandardCharsets.UTF_8))));
        MemoryRecords encrypted = recordsOf((ProduceRequestData) fixture.produceTransformer.transform(produce).message());
        List<Record> encryptedRecords = recordList(encrypted);
        UUID deletedKey = UUID.fromString(new String(header(encryptedRecords.getFirst(), KEY_HEADER).value(),
                StandardCharsets.UTF_8));
        fixture.managementService.delete(deletedKey);

        MessageTransformationResult result = fixture.fetchTransformer.transform("connection-test",
                fetch(TOPIC_ID, TOPIC, encrypted));
        List<Record> decrypted = recordList(recordsOf((FetchResponseData) result.message()));
        assertThat(decrypted.getFirst().hasKey()).isFalse();
        assertThat(decrypted.getFirst().hasValue()).isFalse();
        assertThat(decrypted.getFirst().headers()).extracting(Header::key)
                .containsExactly(KEY_HEADER, IV_HEADER);
        assertThat(copy(decrypted.get(1).key())).isEqualTo("second".getBytes(StandardCharsets.UTF_8));
        assertThat(copy(decrypted.get(1).value())).isEqualTo("two".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void tamperedRecordBecomesTombstoneWithoutPreventingTheFollowingRecord() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        ProduceRequestData produce = produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord("first".getBytes(), "one".getBytes()),
                new SimpleRecord("second".getBytes(), "two".getBytes())));
        MemoryRecords encrypted = recordsOf((ProduceRequestData) fixture.produceTransformer.transform(produce).message());
        List<Record> original = recordList(encrypted);
        byte[] tamperedValue = copy(original.getFirst().value());
        tamperedValue[tamperedValue.length - 1] ^= 1;
        MemoryRecords tampered = MemoryRecords.withRecords(Compression.of(CompressionType.NONE).build(),
                new SimpleRecord(original.getFirst().timestamp(), copy(original.getFirst().key()), tamperedValue,
                        original.getFirst().headers()),
                new SimpleRecord(original.get(1).timestamp(), copy(original.get(1).key()),
                        copy(original.get(1).value()), original.get(1).headers()));

        List<Record> returned = recordList(recordsOf((FetchResponseData) fixture.fetchTransformer
                .transform("connection-test", fetch(TOPIC_ID, TOPIC, tampered)).message()));
        assertThat(returned.getFirst().hasKey()).isFalse();
        assertThat(returned.getFirst().hasValue()).isFalse();
        assertThat(returned.getFirst().headers()).extracting(Header::key)
                .containsExactly(KEY_HEADER, IV_HEADER);
        assertThat(copy(returned.get(1).key())).isEqualTo("second".getBytes());
        assertThat(copy(returned.get(1).value())).isEqualTo("two".getBytes());
    }

    @Test
    void malformedOrDuplicateReservedHeadersProduceTombstonesWithOnlyReservedHeaders() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        MemoryRecords encrypted = recordsOf((ProduceRequestData) fixture.produceTransformer.transform(
                produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                        new SimpleRecord("key".getBytes(), "value".getBytes())))).message());
        Record record = recordList(encrypted).getFirst();
        Header keyHeader = header(record, KEY_HEADER);
        Header ivHeader = header(record, IV_HEADER);
        Header[] duplicate = {new RecordHeader("application", new byte[]{1}), keyHeader, ivHeader,
                new RecordHeader(KEY_HEADER, keyHeader.value())};
        MemoryRecords malformed = MemoryRecords.withRecords(Compression.of(CompressionType.NONE).build(),
                new SimpleRecord(record.timestamp(), copy(record.key()), copy(record.value()), duplicate));

        Record tombstone = recordList(recordsOf((FetchResponseData) fixture.fetchTransformer
                .transform("connection-test", fetch(TOPIC_ID, TOPIC, malformed)).message())).getFirst();
        assertThat(tombstone.hasKey()).isFalse();
        assertThat(tombstone.hasValue()).isFalse();
        assertThat(tombstone.headers()).extracting(Header::key)
                .containsExactly(KEY_HEADER, IV_HEADER, KEY_HEADER);
    }

    @Test
    void producingAfterCryptoShreddingCreatesAUsableNewAssignment() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        ProduceRequestData firstRequest = produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord("stable-key".getBytes(), "first".getBytes())));
        Record firstEncrypted = recordList(recordsOf((ProduceRequestData) fixture.produceTransformer
                .transform(firstRequest).message())).getFirst();
        UUID firstKeyId = UUID.fromString(new String(header(firstEncrypted, KEY_HEADER).value(),
                StandardCharsets.UTF_8));
        fixture.managementService.delete(firstKeyId);

        Record secondEncrypted = recordList(recordsOf((ProduceRequestData) fixture.produceTransformer
                .transform(produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                        new SimpleRecord("stable-key".getBytes(), "second".getBytes())))).message())).getFirst();
        UUID secondKeyId = UUID.fromString(new String(header(secondEncrypted, KEY_HEADER).value(),
                StandardCharsets.UTF_8));
        assertThat(secondKeyId).isNotEqualTo(firstKeyId);
        assertThat(fixture.keyRepository.exists(secondKeyId)).isTrue();
    }

    @Test
    void plaintextFetchIsReturnedUnchangedAndControlBatchesAreSkipped() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        MemoryRecords plaintext = records(CompressionType.NONE, new SimpleRecord("plain".getBytes()));
        MessageTransformationResult plaintextResult = fixture.fetchTransformer.transform("connection-test",
                fetch(TOPIC_ID, TOPIC, plaintext));
        assertThat(plaintextResult.changed()).isFalse();

        MemoryRecords control = MemoryRecords.withEndTransactionMarker(3L, 7L, (short) 2,
                new EndTransactionMarker(ControlRecordType.COMMIT, 4));
        MessageTransformationResult controlResult = fixture.produceTransformer.transform(
                produce(null, "unresolved-control-topic", control));
        assertThat(controlResult.changed()).isFalse();
        assertThat(recordsOf((ProduceRequestData) controlResult.message()).buffer())
                .isEqualTo(control.buffer());
    }

    @Test
    void reservedProducerHeadersAreOverwrittenAndMissingTopicIdsFailClosed() {
        CryptoTransformTestFixture fixture = new CryptoTransformTestFixture();
        ProduceRequestData conflict = produce(TOPIC_ID, TOPIC, records(CompressionType.NONE,
                new SimpleRecord(0L, (byte[]) null, (byte[]) null,
                        new Header[]{new RecordHeader(KEY_HEADER, new byte[]{1})})));
        Record encrypted = recordsOf((ProduceRequestData) fixture.produceTransformer.transform(conflict)
                .message()).records().iterator().next();
        assertThat(encrypted.headers()).extracting(Header::key)
                .containsExactly(KEY_HEADER, IV_HEADER);
        assertThat(header(encrypted, KEY_HEADER).value()).isNotEqualTo(new byte[]{1});

        ProduceRequestData unresolved = produce(null, "unknown", records(CompressionType.NONE,
                new SimpleRecord("value".getBytes())));
        assertThatThrownBy(() -> fixture.produceTransformer.transform(unresolved))
                .isInstanceOf(BackendException.class);
    }

    private static Stream<CompressionType> compressionTypes() {
        return Stream.of(CompressionType.NONE, CompressionType.GZIP, CompressionType.SNAPPY,
                CompressionType.LZ4, CompressionType.ZSTD);
    }

    private ProduceRequestData produce(UUID topicId, String topic, MemoryRecords records) {
        Uuid kafkaTopicId = topicId == null ? Uuid.ZERO_UUID
                : new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
        ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData()
                .setName(topic).setTopicId(kafkaTopicId)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(2).setRecords(records)));
        return new ProduceRequestData().setAcks((short) 1).setTimeoutMs(30_000)
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(List.of(topicData).iterator()));
    }

    private FetchResponseData fetch(UUID topicId, String topic, MemoryRecords records) {
        Uuid kafkaTopicId = new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
        return new FetchResponseData().setResponses(List.of(new FetchResponseData.FetchableTopicResponse()
                .setTopic(topic).setTopicId(kafkaTopicId)
                .setPartitions(List.of(new FetchResponseData.PartitionData()
                        .setPartitionIndex(2).setRecords(records)))));
    }

    private MemoryRecords records(CompressionType compressionType, SimpleRecord... records) {
        return MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 42L,
                Compression.of(compressionType).build(), TimestampType.CREATE_TIME,
                91L, (short) 3, 17, 5, true, records);
    }

    private MemoryRecords recordsOf(ProduceRequestData produce) {
        return (MemoryRecords) produce.topicData().iterator().next().partitionData().getFirst().records();
    }

    private MemoryRecords recordsOf(FetchResponseData fetch) {
        return (MemoryRecords) fetch.responses().getFirst().partitions().getFirst().records();
    }

    private DefaultRecordBatch batch(MemoryRecords records) {
        return (DefaultRecordBatch) records.batches().iterator().next();
    }

    private List<Record> recordList(MemoryRecords records) {
        List<Record> result = new ArrayList<>();
        records.records().forEach(result::add);
        return result;
    }

    private Header header(Record record, String name) {
        return Stream.of(record.headers()).filter(value -> name.equals(value.key())).findFirst().orElseThrow();
    }

    private byte[] copy(ByteBuffer value) {
        ByteBuffer duplicate = value.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private void assertBatchMetadata(DefaultRecordBatch actual, DefaultRecordBatch expected) {
        assertThat(actual.baseOffset()).isEqualTo(expected.baseOffset());
        assertThat(actual.partitionLeaderEpoch()).isEqualTo(expected.partitionLeaderEpoch());
        assertThat(actual.timestampType()).isEqualTo(expected.timestampType());
        assertThat(actual.isTransactional()).isEqualTo(expected.isTransactional());
        assertThat(actual.isControlBatch()).isEqualTo(expected.isControlBatch());
        assertThat(actual.producerId()).isEqualTo(expected.producerId());
        assertThat(actual.producerEpoch()).isEqualTo(expected.producerEpoch());
        assertThat(actual.baseSequence()).isEqualTo(expected.baseSequence());
        assertThat(actual.lastOffset()).isEqualTo(expected.lastOffset());
        assertThat(actual.baseTimestamp()).isEqualTo(expected.baseTimestamp());
        assertThat(actual.maxTimestamp()).isEqualTo(expected.maxTimestamp());
    }
}

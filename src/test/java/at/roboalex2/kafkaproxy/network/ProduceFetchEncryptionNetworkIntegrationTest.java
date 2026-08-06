package at.roboalex2.kafkaproxy.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProduceFetchEncryptionNetworkIntegrationTest {
    private static final short VERSION = 13;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static final UUID TOPIC_ID = UUID.fromString("40000000-0000-0000-0000-000000000004");
    @TempDir Path logDirectory;

    @Test
    void brokerReceivesEncryptedProduceAndClientReceivesDecryptedFetchWithPairedLogs() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), logDirectory);
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                byte[] plaintextKey = "customer-7".getBytes(StandardCharsets.UTF_8);
                byte[] plaintextValue = "private-order".getBytes(StandardCharsets.UTF_8);
                byte[] plaintextHeader = "trace-value".getBytes(StandardCharsets.UTF_8);
                ProduceRequestData produce = produceRequest(plaintextKey, plaintextValue, plaintextHeader,
                        new RecordHeader("trace-id", plaintextHeader));
                client.getOutputStream().write(requestFrame(ApiKeys.PRODUCE, VERSION, 101, produce));
                client.getOutputStream().flush();

                ProduceRequestData encryptedProduce = parseProduce(readFrame(upstream), VERSION, 101);
                MemoryRecords encryptedRecords = (MemoryRecords) encryptedProduce.topicData().iterator().next()
                        .partitionData().getFirst().records();
                Record encrypted = records(encryptedRecords).getFirst();
                assertThat(copy(encrypted.key())).isNotEqualTo(plaintextKey);
                assertThat(copy(encrypted.value())).isNotEqualTo(plaintextValue);
                assertThat(encrypted.headers()[0].key()).startsWith("enc:").isNotEqualTo("trace-id");
                assertThat(encrypted.headers()[0].value()).isNotEqualTo(plaintextHeader);
                assertThat(encrypted.headers()).extracting(Header::key)
                        .contains("encryption-key", "encryption-iv");
                encryptedRecords.batches().iterator().next().ensureValid();

                client.getOutputStream().write(requestFrame(ApiKeys.FETCH, VERSION, 202,
                        new FetchRequestData()));
                client.getOutputStream().flush();
                readFrame(upstream);

                FetchResponseData fetchResponse = fetchResponse(encryptedRecords);
                upstream.getOutputStream().write(responseFrame(ApiKeys.FETCH, VERSION, 202, fetchResponse));
                upstream.getOutputStream().flush();
                FetchResponseData clientFetch = parseFetch(readFrame(client), VERSION, 202);
                MemoryRecords clientRecords = (MemoryRecords) clientFetch.responses().getFirst()
                        .partitions().getFirst().records();
                Record decrypted = records(clientRecords).getFirst();
                assertThat(copy(decrypted.key())).isEqualTo(plaintextKey);
                assertThat(copy(decrypted.value())).isEqualTo(plaintextValue);
                assertThat(decrypted.headers()).extracting(Header::key)
                        .containsExactly("trace-id", "encryption-key", "encryption-iv");
                assertThat(decrypted.headers()[0].value()).isEqualTo(plaintextHeader);
                clientRecords.batches().iterator().next().ensureValid();
            } finally {
                proxy.close();
            }
        }

        String log = Files.readString(Files.list(logDirectory).findFirst().orElseThrow()
                .resolve("connection.log"));
        assertThat(log).contains(
                "1 C -> B: ProduceRequest Version: 13 ORIGINAL",
                "1 C -> B: ProduceRequest Version: 13 FORWARDED_ENCRYPTED",
                "3 B -> C: FetchResponse Version: 13 ORIGINAL_ENCRYPTED",
                "3 B -> C: FetchResponse Version: 13 FORWARDED_DECRYPTED",
                Base64.getEncoder().encodeToString("private-order".getBytes(StandardCharsets.UTF_8)),
                "encryption-key", "encryption-iv");
    }

    @Test
    void clientReservedHeadersAreOverwrittenWithActualEncryptionMetadata() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), logDirectory);
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                ProduceRequestData produce = produceRequest("plain-key".getBytes(), "plain-value".getBytes(),
                        new byte[]{1}, new RecordHeader("encryption-key", "attacker".getBytes()));
                client.getOutputStream().write(requestFrame(ApiKeys.PRODUCE, VERSION, 303, produce));
                client.getOutputStream().flush();
                Record encrypted = records((MemoryRecords) parseProduce(readFrame(upstream), VERSION, 303)
                        .topicData().iterator().next().partitionData().getFirst().records()).getFirst();
                assertThat(encrypted.headers()).extracting(Header::key)
                        .containsExactly("encryption-key", "encryption-iv");
                assertThat(new String(encrypted.headers()[0].value(), StandardCharsets.UTF_8))
                        .isNotEqualTo("attacker");
                UUID.fromString(new String(encrypted.headers()[0].value(), StandardCharsets.UTF_8));
            } finally {
                proxy.close();
            }
        }
    }

    @Test
    void keyStoreFailureClosesPairWithoutForwardingPlaintext() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), logDirectory);
            proxy.crypto().keyRepository.failWrites = true;
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                ProduceRequestData produce = produceRequest("plain-key".getBytes(), "plain-value".getBytes(),
                        new byte[]{1}, new RecordHeader("trace-id", "trace".getBytes()));
                client.getOutputStream().write(requestFrame(ApiKeys.PRODUCE, VERSION, 304, produce));
                client.getOutputStream().flush();
                assertThat(client.getInputStream().read()).isEqualTo(-1);
                assertThat(upstream.getInputStream().read()).isEqualTo(-1);
            } finally {
                proxy.close();
            }
        }
        String log = Files.readString(Files.list(logDirectory).findFirst().orElseThrow()
                .resolve("connection.log"));
        assertThat(log).contains("ProduceRequest transformation failed");
    }

    @Test
    void nameOnlyProduceVersionUsesObservedTopicIdentity() throws Exception {
        short nameOnlyVersion = 12;
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), logDirectory);
             Socket client = connect(proxy)) {
            Socket upstream = broker.awaitConnection(TIMEOUT);
            proxy.crypto().topicIdentityResolver.observe("orders", kafkaUuid(TOPIC_ID));
            byte[] plaintextKey = "plain-key".getBytes(StandardCharsets.UTF_8);
            byte[] frame = requestFrame(ApiKeys.PRODUCE, nameOnlyVersion, 404,
                    produceRequest("plain-key".getBytes(), "plain-value".getBytes(),
                            new byte[]{1}, new RecordHeader("trace-id", "plain-header".getBytes())));
            client.getOutputStream().write(frame);
            client.getOutputStream().flush();
            ProduceRequestData encrypted = parseProduce(readFrame(upstream), nameOnlyVersion, 404);
            Record record = records((MemoryRecords) encrypted.topicData().iterator().next()
                    .partitionData().getFirst().records()).getFirst();
            assertThat(copy(record.key())).isNotEqualTo(plaintextKey);
            assertThat(record.headers()).extracting(Header::key)
                    .contains("encryption-key", "encryption-iv");
        }
    }

    @Test
    void nameOnlyFetchVersionUsesObservedTopicIdentity() throws Exception {
        short nameOnlyVersion = 12;
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), logDirectory);
             Socket client = connect(proxy)) {
            Socket upstream = broker.awaitConnection(TIMEOUT);
            ProduceRequestData encryptedProduce = (ProduceRequestData) proxy.crypto().produceTransformer
                    .transform(produceRequest("plain-key".getBytes(), "plain-value".getBytes(),
                            new byte[]{1}, new RecordHeader("trace-id", "plain-header".getBytes())))
                    .message();
            MemoryRecords encryptedRecords = (MemoryRecords) encryptedProduce.topicData().iterator().next()
                    .partitionData().getFirst().records();

            client.getOutputStream().write(requestFrame(ApiKeys.FETCH, nameOnlyVersion, 405,
                    new FetchRequestData()));
            client.getOutputStream().flush();
            readFrame(upstream);

            byte[] response = responseFrame(ApiKeys.FETCH, nameOnlyVersion, 405,
                    fetchResponse(encryptedRecords));
            upstream.getOutputStream().write(response);
            upstream.getOutputStream().flush();
            FetchResponseData decrypted = parseFetch(readFrame(client), nameOnlyVersion, 405);
            Record record = records((MemoryRecords) decrypted.responses().getFirst()
                    .partitions().getFirst().records()).getFirst();
            assertThat(copy(record.key())).isEqualTo("plain-key".getBytes(StandardCharsets.UTF_8));
            assertThat(copy(record.value())).isEqualTo("plain-value".getBytes(StandardCharsets.UTF_8));
        }
    }

    private ProduceRequestData produceRequest(byte[] key, byte[] value, byte[] headerValue, Header header) {
        MemoryRecords records = MemoryRecords.withRecords(Compression.of(CompressionType.GZIP).build(),
                new SimpleRecord(1_000L, key, value, new Header[]{header}));
        Uuid topicId = kafkaUuid(TOPIC_ID);
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData()
                .setName("orders").setTopicId(topicId)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0).setRecords(records)));
        return new ProduceRequestData().setAcks((short) 1).setTimeoutMs(10_000)
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(List.of(topic).iterator()));
    }

    private FetchResponseData fetchResponse(MemoryRecords records) {
        return new FetchResponseData().setThrottleTimeMs(7).setResponses(List.of(
                new FetchResponseData.FetchableTopicResponse().setTopic("orders").setTopicId(kafkaUuid(TOPIC_ID))
                        .setPartitions(List.of(new FetchResponseData.PartitionData().setPartitionIndex(0)
                                .setHighWatermark(1).setRecords(records)))));
    }

    private ProduceRequestData parseProduce(byte[] frame, short version, int correlationId) {
        ByteBuffer payload = payload(frame);
        RequestHeader header = RequestHeader.parse(payload);
        assertThat(header.correlationId()).isEqualTo(correlationId);
        return new ProduceRequestData(new ByteBufferAccessor(payload), version);
    }

    private FetchResponseData parseFetch(byte[] frame, short version, int correlationId) {
        ByteBuffer payload = payload(frame);
        ResponseHeader header = ResponseHeader.parse(payload, ApiKeys.FETCH.responseHeaderVersion(version));
        assertThat(header.correlationId()).isEqualTo(correlationId);
        return new FetchResponseData(new ByteBufferAccessor(payload), version);
    }

    private byte[] requestFrame(ApiKeys api, short version, int correlationId, ApiMessage body) {
        RequestHeader header = new RequestHeader(api, version, "network-test", correlationId);
        return frame(serialized(header.data(), header.headerVersion()), serialized(body, version));
    }

    private byte[] responseFrame(ApiKeys api, short version, int correlationId, ApiMessage body) {
        ResponseHeader header = new ResponseHeader(correlationId, api.responseHeaderVersion(version));
        return frame(serialized(header.data(), header.headerVersion()), serialized(body, version));
    }

    private ByteBuffer serialized(ApiMessage message, short version) {
        return MessageUtil.toByteBufferAccessor(message, version).buffer();
    }

    private byte[] frame(ByteBuffer header, ByteBuffer body) {
        int size = header.remaining() + body.remaining();
        return ByteBuffer.allocate(Integer.BYTES + size).putInt(size)
                .put(header.duplicate()).put(body.duplicate()).array();
    }

    private ByteBuffer payload(byte[] frame) {
        ByteBuffer payload = ByteBuffer.wrap(frame);
        assertThat(payload.getInt()).isEqualTo(frame.length - Integer.BYTES);
        return payload;
    }

    private byte[] readFrame(Socket socket) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int size = input.readInt();
        byte[] payload = input.readNBytes(size);
        if (payload.length != size) throw new IOException("Incomplete Kafka frame");
        return ByteBuffer.allocate(Integer.BYTES + size).putInt(size).put(payload).array();
    }

    private List<Record> records(MemoryRecords records) {
        List<Record> values = new ArrayList<>();
        records.records().forEach(values::add);
        return values;
    }

    private byte[] copy(ByteBuffer value) {
        ByteBuffer duplicate = value.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private Uuid kafkaUuid(UUID uuid) {
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private Socket connect(ProxyTestHarness proxy) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(3_000);
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), proxy.getListenPort()), 3_000);
        return socket;
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }
}

package at.roboalex2.kafkaproxy.network;

import static org.assertj.core.api.Assertions.assertThat;

import at.roboalex2.kafkaproxy.config.Endpoint;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetadataRewriteNetworkIntegrationTest {
    private static final short VERSION = 13;
    private static final int CORRELATION_ID = 731;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    @TempDir Path temporaryDirectory;

    @Test
    void rewritesMetadataAndLogsOriginalAndModifiedWithOneMessageNumber() throws Exception {
        Map<Endpoint, Endpoint> mappings = Map.of(
                new Endpoint("broker-a", 9092), new Endpoint("proxy-a", 19092),
                new Endpoint("broker-b", 9093), new Endpoint("proxy-b", 19093));
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), temporaryDirectory, mappings);
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                byte[] request = metadataRequest(VERSION, CORRELATION_ID);
                client.getOutputStream().write(request);
                client.getOutputStream().flush();
                assertThat(readFrame(upstream)).isEqualTo(request);

                MetadataResponseData original = metadataResponse();
                byte[] response = metadataResponseFrame(VERSION, CORRELATION_ID, original);
                upstream.getOutputStream().write(response);
                upstream.getOutputStream().flush();

                byte[] forwarded = readFrame(client);
                assertThat(ByteBuffer.wrap(forwarded).getInt()).isEqualTo(forwarded.length - Integer.BYTES);
                MetadataResponseData parsed = parseMetadataResponse(forwarded, VERSION, CORRELATION_ID);
                assertThat(parsed.brokers().find(1).host()).isEqualTo("proxy-a");
                assertThat(parsed.brokers().find(1).port()).isEqualTo(19092);
                assertThat(parsed.brokers().find(1).nodeId()).isEqualTo(1);
                assertThat(parsed.brokers().find(1).rack()).isEqualTo("rack-a");
                assertThat(parsed.brokers().find(2).host()).isEqualTo("proxy-b");
                assertThat(parsed.brokers().find(2).port()).isEqualTo(19093);
                assertThat(parsed.topics()).isEqualTo(original.topics());
                assertThat(parsed.clusterId()).isEqualTo(original.clusterId());
                assertThat(parsed.controllerId()).isEqualTo(original.controllerId());
                assertThat(parsed.unknownTaggedFields()).isEqualTo(original.unknownTaggedFields());
            } finally {
                proxy.close();
            }

            Path connectionLog = Files.list(temporaryDirectory).findFirst().orElseThrow().resolve("connection.log");
            String log = Files.readString(connectionLog);
            assertThat(log).contains(
                    "2 B -> C: MetadataResponse Version: 13 ORIGINAL",
                    "2 B -> C: MetadataResponse Version: 13 FORWARDED_MODIFIED",
                    "broker-a", "proxy-a");
        }
    }

    @Test
    void rewritesWhenLoggingIsDisabledWithoutCreatingTheConfiguredLogDirectory() throws Exception {
        Path unusedLogDirectory = temporaryDirectory.resolve("disabled");
        Map<Endpoint, Endpoint> mappings = Map.of(
                new Endpoint("broker-a", 9092), new Endpoint("proxy-a", 19092),
                new Endpoint("broker-b", 9093), new Endpoint("proxy-b", 19093));
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), null, mappings);
             Socket client = connect(proxy)) {
            Socket upstream = broker.awaitConnection(TIMEOUT);
            client.getOutputStream().write(metadataRequest(VERSION, CORRELATION_ID));
            client.getOutputStream().flush();
            readFrame(upstream);
            upstream.getOutputStream().write(metadataResponseFrame(VERSION, CORRELATION_ID, metadataResponse()));
            upstream.getOutputStream().flush();
            assertThat(parseMetadataResponse(readFrame(client), VERSION, CORRELATION_ID)
                    .brokers().find(1).host()).isEqualTo("proxy-a");
        }
        assertThat(unusedLogDirectory).doesNotExist();
    }

    @Test
    void missingMappingClosesConnectionWithoutForwardingPartialMetadata() throws Exception {
        Map<Endpoint, Endpoint> incomplete = Map.of(
                new Endpoint("broker-a", 9092), new Endpoint("proxy-a", 19092));
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(
                    freePort(), broker.getPort(), temporaryDirectory, incomplete);
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                client.getOutputStream().write(metadataRequest(VERSION, CORRELATION_ID));
                client.getOutputStream().flush();
                readFrame(upstream);
                upstream.getOutputStream().write(
                        metadataResponseFrame(VERSION, CORRELATION_ID, metadataResponse()));
                upstream.getOutputStream().flush();
                assertThat(client.getInputStream().read()).isEqualTo(-1);
            } finally {
                proxy.close();
            }
        }
        String log = Files.readString(Files.list(temporaryDirectory)
                .findFirst().orElseThrow().resolve("connection.log"));
        assertThat(log).contains("MetadataResponse transformation failed", "broker-b:9093");
    }

    @Test
    void unsupportedMetadataVersionIsForwardedByteForByte() throws Exception {
        short unsupportedVersion = 14;
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), temporaryDirectory);
            try (Socket client = connect(proxy)) {
                Socket upstream = broker.awaitConnection(TIMEOUT);
                byte[] request = ByteBuffer.allocate(12).putInt(8).putShort(ApiKeys.METADATA.id)
                        .putShort(unsupportedVersion).putInt(CORRELATION_ID).array();
                client.getOutputStream().write(request);
                client.getOutputStream().flush();
                assertThat(readFrame(upstream)).isEqualTo(request);
                short responseHeaderVersion = ApiKeys.METADATA.responseHeaderVersion(unsupportedVersion);
                ResponseHeader responseHeader = new ResponseHeader(CORRELATION_ID, responseHeaderVersion);
                byte[] response = frame(serialized(responseHeader.data(), responseHeaderVersion),
                        ByteBuffer.wrap(new byte[]{0x12, 0x34, 0x56, 0x78}));
                upstream.getOutputStream().write(response);
                upstream.getOutputStream().flush();
                assertThat(readFrame(client)).isEqualTo(response);
            } finally {
                proxy.close();
            }
        }
        String log = Files.readString(Files.list(temporaryDirectory)
                .findFirst().orElseThrow().resolve("connection.log"));
        assertThat(log).contains("MetadataResponse Version: 14 (body inspection unsupported)");
    }

    private MetadataResponseData metadataResponse() {
        MetadataResponseData response = new MetadataResponseData().setThrottleTimeMs(19)
                .setClusterId("cluster-one").setControllerId(2);
        MetadataResponseData.MetadataResponseBroker first = new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker-a").setPort(9092).setRack("rack-a");
        first.unknownTaggedFields().add(new RawTaggedField(4, new byte[]{1}));
        response.brokers().add(first);
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(2).setHost("broker-b").setPort(9093).setRack("rack-b"));
        MetadataResponseData.MetadataResponsePartition partition =
                new MetadataResponseData.MetadataResponsePartition().setPartitionIndex(3).setLeaderId(2)
                        .setLeaderEpoch(5).setReplicaNodes(List.of(1, 2)).setIsrNodes(List.of(2))
                        .setOfflineReplicas(List.of(1));
        partition.unknownTaggedFields().add(new RawTaggedField(6, new byte[]{2}));
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
                .setName("orders").setIsInternal(false).setPartitions(List.of(partition));
        topic.unknownTaggedFields().add(new RawTaggedField(7, new byte[]{3}));
        response.topics().add(topic);
        response.unknownTaggedFields().add(new RawTaggedField(8, new byte[]{4, 5}));
        return response;
    }

    private byte[] metadataRequest(short version, int correlationId) {
        RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "fixture", correlationId);
        return frame(serialized(header.data(), header.headerVersion()),
                serialized(new MetadataRequestData(), version));
    }

    private byte[] metadataResponseFrame(short version, int correlationId, MetadataResponseData response) {
        ResponseHeader header = new ResponseHeader(correlationId, ApiKeys.METADATA.responseHeaderVersion(version));
        return frame(serialized(header.data(), header.headerVersion()), serialized(response, version));
    }

    private MetadataResponseData parseMetadataResponse(byte[] frame, short version, int correlationId) {
        ByteBuffer payload = ByteBuffer.wrap(frame);
        payload.getInt();
        ResponseHeader header = ResponseHeader.parse(payload, ApiKeys.METADATA.responseHeaderVersion(version));
        assertThat(header.correlationId()).isEqualTo(correlationId);
        MetadataResponseData response = new MetadataResponseData(new ByteBufferAccessor(payload), version);
        assertThat(payload.remaining()).isZero();
        return response;
    }

    private ByteBuffer serialized(ApiMessage message, short version) {
        return MessageUtil.toByteBufferAccessor(message, version).buffer();
    }

    private byte[] frame(ByteBuffer header, ByteBuffer body) {
        int size = header.remaining() + body.remaining();
        return ByteBuffer.allocate(Integer.BYTES + size).putInt(size)
                .put(header.duplicate()).put(body.duplicate()).array();
    }

    private Socket connect(ProxyTestHarness proxy) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(3_000);
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), proxy.getListenPort()), 3_000);
        return socket;
    }

    private byte[] readFrame(Socket socket) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int size = input.readInt();
        byte[] payload = input.readNBytes(size);
        if (payload.length != size) throw new IOException("Incomplete Kafka frame");
        return ByteBuffer.allocate(Integer.BYTES + size).putInt(size).put(payload).array();
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }
}

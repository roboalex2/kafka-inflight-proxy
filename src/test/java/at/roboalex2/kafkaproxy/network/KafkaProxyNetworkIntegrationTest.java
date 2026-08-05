package at.roboalex2.kafkaproxy.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaProxyNetworkIntegrationTest {
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(3);
    @TempDir Path temporaryDirectory;

    @Test
    void logsBothDirectionsWithOneMonotonicConnectionCounterWithoutChangingFrames() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort(), temporaryDirectory);
            try (Socket client = connect(proxy)) {
                Socket brokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);
                byte[] request = frame("unknown-request");
                byte[] response = frame("unknown-response");
                client.getOutputStream().write(request);
                client.getOutputStream().flush();
                assertThat(readFrame(brokerConnection.getInputStream())).isEqualTo(request);
                brokerConnection.getOutputStream().write(response);
                brokerConnection.getOutputStream().flush();
                assertThat(readFrame(client.getInputStream())).isEqualTo(response);
            } finally {
                proxy.close();
            }

            Path connectionDirectory = Files.list(temporaryDirectory).findFirst().orElseThrow();
            assertThat(Files.list(temporaryDirectory).toList()).containsExactly(connectionDirectory);
            assertThat(Files.list(connectionDirectory).toList())
                    .containsExactly(connectionDirectory.resolve("connection.log"));
            String log = Files.readString(connectionDirectory.resolve("connection.log"));
            assertThat(log).contains("1 C -> B:", "2 B -> C:", "Unknown Kafka frame");
        }
    }

    @Test
    void relaysFramesByteForByteInBothDirectionsWithoutRequestResponseLockstep() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort());
             Socket client = connect(proxy)) {
            Socket brokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);
            byte[] firstRequest = frame("request-one");
            byte[] secondRequest = frame("request-two");

            client.getOutputStream().write(firstRequest);
            client.getOutputStream().write(secondRequest);
            client.getOutputStream().flush();

            assertThat(readFrame(brokerConnection.getInputStream())).isEqualTo(firstRequest);
            assertThat(readFrame(brokerConnection.getInputStream())).isEqualTo(secondRequest);

            byte[] response = frame("response-one");
            brokerConnection.getOutputStream().write(response);
            brokerConnection.getOutputStream().flush();
            assertThat(readFrame(client.getInputStream())).isEqualTo(response);
        }
    }

    @Test
    void handlesMultipleClientConnectionsConcurrently() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort());
             Socket firstClient = connect(proxy)) {
            Socket firstBrokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);
            try (Socket secondClient = connect(proxy)) {
                Socket secondBrokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);
                byte[] firstFrame = frame("first-client");
                byte[] secondFrame = frame("second-client");

                firstClient.getOutputStream().write(firstFrame);
                secondClient.getOutputStream().write(secondFrame);
                firstClient.getOutputStream().flush();
                secondClient.getOutputStream().flush();

                assertThat(readFrame(firstBrokerConnection.getInputStream())).isEqualTo(firstFrame);
                assertThat(readFrame(secondBrokerConnection.getInputStream())).isEqualTo(secondFrame);
                assertThat(proxy.connectionCount()).isEqualTo(2);
            }
        }
    }

    @Test
    void closingClientClosesItsBrokerConnection() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort())) {
            Socket client = connect(proxy);
            Socket brokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);

            client.close();

            assertThat(brokerConnection.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    void closingBrokerClosesItsClientConnection() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker();
             ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort());
             Socket client = connect(proxy)) {
            Socket brokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);

            brokerConnection.close();

            assertThat(client.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    void failedUpstreamConnectionClosesClient() throws Exception {
        int unusedBrokerPort = freePort();
        try (ProxyTestHarness proxy = new ProxyTestHarness(freePort(), unusedBrokerPort);
             Socket client = connect(proxy)) {
            assertThat(client.getInputStream().read()).isEqualTo(-1);
            assertThat(proxy.connectionCount()).isZero();
        }
    }

    @Test
    void gracefulShutdownClosesListenerAndActivePairs() throws Exception {
        try (FakeKafkaBroker broker = new FakeKafkaBroker()) {
            ProxyTestHarness proxy = new ProxyTestHarness(freePort(), broker.getPort());
            try (Socket client = connect(proxy)) {
                Socket brokerConnection = broker.awaitConnection(CONNECTION_TIMEOUT);
                assertThat(proxy.getServer().isRunning()).isTrue();

                proxy.close();

                assertThat(proxy.getServer().isRunning()).isFalse();
                assertThat(proxy.connectionCount()).isZero();
                assertThat(client.getInputStream().read()).isEqualTo(-1);
                assertThat(brokerConnection.getInputStream().read()).isEqualTo(-1);
            }
        }
    }

    private Socket connect(ProxyTestHarness proxy) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(3_000);
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), proxy.getListenPort()), 3_000);
        return socket;
    }

    private byte[] readFrame(InputStream inputStream) throws IOException {
        DataInputStream dataInput = new DataInputStream(inputStream);
        int payloadLength = dataInput.readInt();
        byte[] payload = dataInput.readNBytes(payloadLength);
        if (payload.length != payloadLength) {
            throw new IOException("Connection closed before a complete Kafka frame was read");
        }
        return ByteBuffer.allocate(Integer.BYTES + payloadLength)
                .putInt(payloadLength)
                .put(payload)
                .array();
    }

    private byte[] frame(String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Integer.BYTES + bytes.length).putInt(bytes.length).put(bytes).array();
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }
}

package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.inspect.ConnectionProtocolContextFactory;
import at.roboalex2.kafkaproxy.protocol.inspect.TransformationExecutor;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.KafkaProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataEndpointTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.CryptoTransformTestFixture;
import tools.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

final class ProxyTestHarness implements AutoCloseable {
    private final int listenPort;
    private final ConnectionRegistry connectionRegistry;
    private final NettyKafkaProxyServer server;
    private final TransformationExecutor inspectionExecutor;
    private final CryptoTransformTestFixture crypto;

    ProxyTestHarness(int listenPort, int brokerPort) {
        this(listenPort, brokerPort, null);
    }

    ProxyTestHarness(int listenPort, int brokerPort, Path logDirectory) {
        this(listenPort, brokerPort, logDirectory, Map.of(
                new Endpoint("127.0.0.1", brokerPort), new Endpoint("127.0.0.1", listenPort)));
    }

    ProxyTestHarness(int listenPort, int brokerPort, Path logDirectory,
                     Map<Endpoint, Endpoint> brokerMappings) {
        this.listenPort = listenPort;
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.setListenAddress(new Endpoint("127.0.0.1", listenPort));
        properties.setUpstreamBrokerAddress(new Endpoint("127.0.0.1", brokerPort));
        properties.setBrokerProxyAddresses(new LinkedHashMap<>(brokerMappings));
        if (logDirectory != null) {
            properties.getRequestLogging().setEnabled(true);
            properties.getRequestLogging().setBaseDirectory(logDirectory);
        }

        connectionRegistry = new ConnectionRegistry();
        ChannelBackpressureController backpressureController = new ChannelBackpressureController();
        BrokerChannelInitializer brokerInitializer = new BrokerChannelInitializer(properties);
        NettyBrokerConnectionFactory brokerConnectionFactory =
                new NettyBrokerConnectionFactory(properties, brokerInitializer);
        inspectionExecutor = new TransformationExecutor(properties);
        ProtocolModelMapper modelMapper = new ProtocolModelMapper();
        crypto = new CryptoTransformTestFixture();
        ConnectionProtocolContextFactory protocolContextFactory = new ConnectionProtocolContextFactory(
                new ProtocolParser(modelMapper), new ConnectionLogWriterFactory(properties),
                new ObjectMapper(), inspectionExecutor, new MetadataEndpointTransformer(properties),
                crypto.produceTransformer, crypto.fetchTransformer, crypto.topicIdentityResolver,
                new KafkaProtocolMessageSerializer(), modelMapper, properties);
        ClientChannelInitializer clientInitializer = new ClientChannelInitializer(
                brokerConnectionFactory, connectionRegistry, backpressureController,
                protocolContextFactory, properties);
        server = new NettyKafkaProxyServer(properties, clientInitializer, connectionRegistry);
        server.start();
    }

    NettyKafkaProxyServer getServer() {
        return server;
    }

    int connectionCount() {
        return connectionRegistry.size();
    }

    int getListenPort() {
        return listenPort;
    }

    CryptoTransformTestFixture crypto() { return crypto; }

    @Override
    public void close() {
        server.stop();
        inspectionExecutor.shutdown();
    }
}

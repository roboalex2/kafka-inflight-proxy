package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.inspect.ConnectionProtocolContextFactory;
import at.roboalex2.kafkaproxy.protocol.inspect.VirtualThreadExecutor;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.KafkaProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataEndpointTransformer;
import tools.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

final class ProxyTestHarness implements AutoCloseable {
    private final int listenPort;
    private final DefaultConnectionRegistry connectionRegistry;
    private final NettyKafkaProxyServer server;
    private final VirtualThreadExecutor inspectionExecutor;

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

        connectionRegistry = new DefaultConnectionRegistry();
        ChannelBackpressureController backpressureController = new ChannelBackpressureController();
        BrokerChannelInitializer brokerInitializer = new BrokerChannelInitializer(properties);
        BrokerConnectionFactory brokerConnectionFactory =
                new NettyBrokerConnectionFactory(properties, brokerInitializer);
        inspectionExecutor = new VirtualThreadExecutor();
        ProtocolModelMapper modelMapper = new ProtocolModelMapper();
        ConnectionProtocolContextFactory protocolContextFactory = new ConnectionProtocolContextFactory(
                new ProtocolParser(modelMapper), new ConnectionLogWriterFactory(properties),
                new ObjectMapper(), inspectionExecutor, new MetadataEndpointTransformer(properties),
                new KafkaProtocolMessageSerializer(), modelMapper);
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

    @Override
    public void close() {
        server.stop();
        inspectionExecutor.shutdown();
    }
}

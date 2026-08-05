package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;

final class ProxyTestHarness implements AutoCloseable {
    private final int listenPort;
    private final DefaultConnectionRegistry connectionRegistry;
    private final NettyKafkaProxyServer server;

    ProxyTestHarness(int listenPort, int brokerPort) {
        this.listenPort = listenPort;
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.setListenAddress(new Endpoint("127.0.0.1", listenPort));
        properties.setUpstreamBrokerAddress(new Endpoint("127.0.0.1", brokerPort));

        connectionRegistry = new DefaultConnectionRegistry();
        ChannelBackpressureController backpressureController = new ChannelBackpressureController();
        BrokerChannelInitializer brokerInitializer = new BrokerChannelInitializer(properties);
        BrokerConnectionFactory brokerConnectionFactory =
                new NettyBrokerConnectionFactory(properties, brokerInitializer);
        ClientChannelInitializer clientInitializer = new ClientChannelInitializer(
                brokerConnectionFactory, connectionRegistry, backpressureController, properties);
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
    }
}

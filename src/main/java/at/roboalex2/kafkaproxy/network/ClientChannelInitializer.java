package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.protocol.frame.KafkaFrameDecoder;
import at.roboalex2.kafkaproxy.protocol.inspect.ConnectionProtocolContext;
import at.roboalex2.kafkaproxy.protocol.inspect.ConnectionProtocolContextFactory;
import at.roboalex2.kafkaproxy.protocol.inspect.ProtocolInspectionHandler;
import at.roboalex2.kafkaproxy.protocol.inspect.TrafficDirection;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.springframework.stereotype.Component;

@Component
public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final BrokerConnectionFactory brokerConnectionFactory;
    private final ConnectionRegistry connectionRegistry;
    private final ChannelBackpressureController backpressureController;
    private final ConnectionProtocolContextFactory protocolContextFactory;
    private final int maxFrameSizeBytes;

    public ClientChannelInitializer(BrokerConnectionFactory brokerConnectionFactory,
                                    ConnectionRegistry connectionRegistry,
                                    ChannelBackpressureController backpressureController,
                                    ConnectionProtocolContextFactory protocolContextFactory,
                                    KafkaProxyProperties properties) {
        this.brokerConnectionFactory = brokerConnectionFactory;
        this.connectionRegistry = connectionRegistry;
        this.backpressureController = backpressureController;
        this.protocolContextFactory = protocolContextFactory;
        this.maxFrameSizeBytes = properties.getProtocol().getMaxFrameSizeBytes();
    }

    @Override
    protected void initChannel(SocketChannel clientChannel) {
        clientChannel.pipeline().addLast("kafkaFrameDecoder", new KafkaFrameDecoder(maxFrameSizeBytes));
        if (!connectionRegistry.registerPending(clientChannel)) {
            return;
        }

        ChannelFuture brokerConnectFuture = brokerConnectionFactory.connect(clientChannel);
        clientChannel.closeFuture().addListener(ignored -> {
            connectionRegistry.unregisterPending(clientChannel);
            if (!brokerConnectFuture.isDone()) {
                brokerConnectFuture.cancel(false);
            } else if (brokerConnectFuture.isSuccess()) {
                brokerConnectFuture.channel().close();
            }
        });
        brokerConnectFuture.addListener(connectFuture -> {
            if (!connectFuture.isSuccess() || !clientChannel.isActive()) {
                clientChannel.close();
                if (connectFuture.isSuccess()) {
                    brokerConnectFuture.channel().close();
                }
                return;
            }

            ConnectionProtocolContext protocolContext = protocolContextFactory.create(clientChannel);
            DefaultConnectionPair connectionPair = new DefaultConnectionPair(
                    clientChannel, brokerConnectFuture.channel(), connectionRegistry, protocolContext);
            connectionRegistry.unregisterPending(clientChannel);
            if (!connectionRegistry.register(connectionPair)) {
                connectionPair.close();
                return;
            }
            connectionPair.activateCloseCoupling();

            clientChannel.pipeline().addLast("protocolInspection",
                    new ProtocolInspectionHandler(protocolContext, TrafficDirection.CLIENT_TO_BROKER,
                            connectionPair));
            clientChannel.pipeline().addLast("clientToBroker",
                    new ClientToBrokerHandler(connectionPair, backpressureController));
            brokerConnectFuture.channel().pipeline().addLast("protocolInspection",
                    new ProtocolInspectionHandler(protocolContext, TrafficDirection.BROKER_TO_CLIENT,
                            connectionPair));
            brokerConnectFuture.channel().pipeline().addLast("brokerToClient",
                    new BrokerToClientHandler(connectionPair, backpressureController));

            clientChannel.config().setAutoRead(true);
            brokerConnectFuture.channel().config().setAutoRead(true);
        });
    }
}

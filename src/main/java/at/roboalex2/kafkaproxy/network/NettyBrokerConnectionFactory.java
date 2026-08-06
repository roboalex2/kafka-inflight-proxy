package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.springframework.stereotype.Component;

@Component
public class NettyBrokerConnectionFactory {
    private final Endpoint upstreamBroker;
    private final int connectTimeoutMillis;
    private final BrokerChannelInitializer channelInitializer;

    public NettyBrokerConnectionFactory(KafkaProxyProperties properties,
                                        BrokerChannelInitializer channelInitializer) {
        this.upstreamBroker = properties.getUpstreamBrokerAddress();
        this.connectTimeoutMillis = properties.getServer().getConnectTimeoutMillis();
        this.channelInitializer = channelInitializer;
    }

    public ChannelFuture connect(Channel clientChannel) {
        Bootstrap bootstrap = new Bootstrap()
                .group(clientChannel.eventLoop())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .handler(channelInitializer);
        return bootstrap.connect(upstreamBroker.getHost(), upstreamBroker.getPort());
    }
}

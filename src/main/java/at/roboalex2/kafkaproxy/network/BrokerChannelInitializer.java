package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.protocol.frame.KafkaFrameDecoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.springframework.stereotype.Component;

@Component
public class BrokerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final int maxFrameSizeBytes;

    public BrokerChannelInitializer(KafkaProxyProperties properties) {
        this.maxFrameSizeBytes = properties.getProtocol().getMaxFrameSizeBytes();
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        channel.pipeline().addLast("kafkaFrameDecoder", new KafkaFrameDecoder(maxFrameSizeBytes));
    }
}

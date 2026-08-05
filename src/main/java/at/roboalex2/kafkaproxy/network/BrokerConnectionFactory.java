package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/** Creates the single upstream connection associated with an accepted client connection. */
public interface BrokerConnectionFactory {
    ChannelFuture connect(Channel clientChannel);
}

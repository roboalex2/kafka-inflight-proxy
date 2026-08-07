package at.roboalex2.kafkaproxy.network;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoop;
import org.junit.jupiter.api.Test;

class ChannelBackpressureControllerTest {
    @Test
    void pausesBothChannelsWhenTheSharedTransformationQueueIsFull() {
        Channels channels = channels(true, true, true, true);
        ConnectionPair pair = pair(channels, true);

        new ChannelBackpressureController().updateConnectionReading(pair);

        verify(channels.clientConfig).setAutoRead(false);
        verify(channels.brokerConfig).setAutoRead(false);
    }

    @Test
    void destinationBackpressureOnlyPausesItsSource() {
        Channels channels = channels(true, true, true, false);
        ConnectionPair pair = pair(channels, false);

        new ChannelBackpressureController().updateConnectionReading(pair);

        verify(channels.clientConfig).setAutoRead(false);
        verify(channels.brokerConfig, never()).setAutoRead(false);
    }

    @Test
    void writableDestinationsDoNotOverrideTransformationBackpressure() {
        Channels channels = channels(false, false, true, true);
        ConnectionPair pair = pair(channels, true);

        new ChannelBackpressureController().updateConnectionReading(pair);

        verify(channels.clientConfig, never()).setAutoRead(true);
        verify(channels.brokerConfig, never()).setAutoRead(true);
    }

    @Test
    void resumesBothChannelsOnlyWhenAllPauseConditionsAreClear() {
        Channels channels = channels(false, false, true, true);
        ConnectionPair pair = pair(channels, false);

        new ChannelBackpressureController().updateConnectionReading(pair);

        verify(channels.clientConfig).setAutoRead(true);
        verify(channels.brokerConfig).setAutoRead(true);
    }

    private ConnectionPair pair(Channels channels, boolean transformationQueueAtCapacity) {
        ConnectionPair pair = mock(ConnectionPair.class);
        when(pair.getClientChannel()).thenReturn(channels.client);
        when(pair.getBrokerChannel()).thenReturn(channels.broker);
        when(pair.isTransformationQueueAtCapacity()).thenReturn(transformationQueueAtCapacity);
        return pair;
    }

    private Channels channels(boolean clientAutoRead, boolean brokerAutoRead,
                              boolean clientWritable, boolean brokerWritable) {
        Channel client = channel(clientAutoRead, clientWritable);
        Channel broker = channel(brokerAutoRead, brokerWritable);
        return new Channels(client, client.config(), broker, broker.config());
    }

    private Channel channel(boolean autoRead, boolean writable) {
        Channel channel = mock(Channel.class);
        ChannelConfig config = mock(ChannelConfig.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.config()).thenReturn(config);
        when(channel.eventLoop()).thenReturn(eventLoop);
        when(channel.isOpen()).thenReturn(true);
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(writable);
        when(config.isAutoRead()).thenReturn(autoRead);
        when(eventLoop.inEventLoop()).thenReturn(true);
        return channel;
    }

    private record Channels(Channel client, ChannelConfig clientConfig,
                            Channel broker, ChannelConfig brokerConfig) { }
}

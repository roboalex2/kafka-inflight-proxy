package at.roboalex2.kafkaproxy.network;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import org.junit.jupiter.api.Test;

class ChannelBackpressureControllerTest {
    @Test
    void pausesSourceWhenDestinationIsNotWritable() {
        Channel source = mock(Channel.class);
        ChannelConfig sourceConfig = mock(ChannelConfig.class);
        Channel destination = mock(Channel.class);
        when(source.isOpen()).thenReturn(true);
        when(source.config()).thenReturn(sourceConfig);
        when(sourceConfig.isAutoRead()).thenReturn(true);
        when(destination.isActive()).thenReturn(true);
        when(destination.isWritable()).thenReturn(false);
        ChannelBackpressureController controller = new ChannelBackpressureController();

        controller.updateSourceReading(source, destination);

        verify(sourceConfig).setAutoRead(false);
    }

    @Test
    void resumesSourceWhenDestinationDropsBelowItsLowWatermark() {
        Channel source = mock(Channel.class);
        ChannelConfig sourceConfig = mock(ChannelConfig.class);
        Channel destination = mock(Channel.class);
        when(source.isOpen()).thenReturn(true);
        when(source.config()).thenReturn(sourceConfig);
        when(sourceConfig.isAutoRead()).thenReturn(false);
        when(destination.isActive()).thenReturn(true);
        when(destination.isWritable()).thenReturn(true);
        ChannelBackpressureController controller = new ChannelBackpressureController();

        controller.updateSourceReading(source, destination);

        verify(sourceConfig).setAutoRead(true);
    }
}

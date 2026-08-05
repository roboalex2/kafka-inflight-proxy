package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import org.springframework.stereotype.Component;

/** Pauses a source channel while its destination has crossed the write high-water mark. */
@Component
public class ChannelBackpressureController {
    public void updateSourceReading(Channel sourceChannel, Channel destinationChannel) {
        setSourceReading(sourceChannel, destinationChannel.isActive() && destinationChannel.isWritable());
    }

    void setSourceReading(Channel sourceChannel, boolean enabled) {
        if (sourceChannel.isOpen() && sourceChannel.config().isAutoRead() != enabled) {
            sourceChannel.config().setAutoRead(enabled);
        }
    }
}

package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import org.springframework.stereotype.Component;

/** The single owner of auto-read state for both channels in a connection pair. */
@Component
public class ChannelBackpressureController {
    public void updateConnectionReading(ConnectionPair connectionPair) {
        updateSourceReading(connectionPair.getClientChannel(), connectionPair.getBrokerChannel(), connectionPair);
        updateSourceReading(connectionPair.getBrokerChannel(), connectionPair.getClientChannel(), connectionPair);
    }

    private void updateSourceReading(Channel sourceChannel, Channel destinationChannel,
                                     ConnectionPair connectionPair) {
        Runnable update = () -> {
            boolean enabled = sourceChannel.isActive()
                    && destinationChannel.isActive()
                    && destinationChannel.isWritable()
                    && !connectionPair.isTransformationQueueAtCapacity();
            if (sourceChannel.isOpen() && sourceChannel.config().isAutoRead() != enabled) {
                sourceChannel.config().setAutoRead(enabled);
            }
        };
        if (sourceChannel.eventLoop().inEventLoop()) {
            update.run();
        } else {
            sourceChannel.eventLoop().execute(update);
        }
    }
}

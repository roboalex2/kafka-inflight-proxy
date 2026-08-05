package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;

public class BrokerToClientHandler extends AbstractFrameRelayHandler {
    public BrokerToClientHandler(ConnectionPair connectionPair,
                                 ChannelBackpressureController backpressureController) {
        super(connectionPair, backpressureController);
    }

    @Override
    protected Channel destination(ConnectionPair pair) {
        return pair.getClientChannel();
    }
}

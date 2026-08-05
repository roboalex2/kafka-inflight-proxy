package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;

public class ClientToBrokerHandler extends AbstractFrameRelayHandler {
    public ClientToBrokerHandler(ConnectionPair connectionPair,
                                 ChannelBackpressureController backpressureController) {
        super(connectionPair, backpressureController);
    }

    @Override
    protected Channel destination(ConnectionPair pair) {
        return pair.getBrokerChannel();
    }
}

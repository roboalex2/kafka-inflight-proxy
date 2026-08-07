package at.roboalex2.kafkaproxy.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

/** Transfers ownership of each complete frame to the paired destination channel. */
abstract class AbstractFrameRelayHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionPair connectionPair;
    private final ChannelBackpressureController backpressureController;

    AbstractFrameRelayHandler(ConnectionPair connectionPair,
                              ChannelBackpressureController backpressureController) {
        this.connectionPair = connectionPair;
        this.backpressureController = backpressureController;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object frame) {
        Channel destination = destination(connectionPair);
        if (!destination.isActive()) {
            ReferenceCountUtil.release(frame);
            connectionPair.close();
            return;
        }

        destination.writeAndFlush(frame).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                connectionPair.close();
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
        connectionPair.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext context) {
        backpressureController.updateConnectionReading(connectionPair);
        context.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        connectionPair.close();
    }

    protected abstract Channel destination(ConnectionPair pair);
}

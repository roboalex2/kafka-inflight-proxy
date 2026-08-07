package at.roboalex2.kafkaproxy.protocol.inspect;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import at.roboalex2.kafkaproxy.network.ChannelBackpressureController;
import at.roboalex2.kafkaproxy.network.ConnectionPair;
import java.util.function.Consumer;

/** Routes both directions through connection-ordered off-event-loop inspection and transformation. */
public class ProtocolInspectionHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionProtocolContext protocolContext;
    private final TrafficDirection direction;
    private final ConnectionPair connectionPair;
    private final ChannelBackpressureController backpressureController;

    public ProtocolInspectionHandler(ConnectionProtocolContext protocolContext, TrafficDirection direction,
                                     ConnectionPair connectionPair,
                                     ChannelBackpressureController backpressureController) {
        this.protocolContext = protocolContext;
        this.direction = direction;
        this.connectionPair = connectionPair;
        this.backpressureController = backpressureController;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof ByteBuf frame) {

            Consumer<ByteBuf> ready = outbound -> context.executor().execute(() -> {
                    if (context.channel().isActive()) {
                        context.fireChannelRead(outbound);
                    } else {
                        ReferenceCountUtil.release(outbound);
                    }
                });
            Runnable completed = () -> context.executor().execute(() -> {
                backpressureController.updateConnectionReading(connectionPair);
            });
            boolean accepted;
            if (direction == TrafficDirection.BROKER_TO_CLIENT) {
                accepted = protocolContext.processBrokerFrame(frame, ready, connectionPair::close, completed);
            } else {
                accepted = protocolContext.processClientFrame(frame, ready, connectionPair::close, completed);
            }
            if (!accepted) {
                connectionPair.close();
            } else {
                backpressureController.updateConnectionReading(connectionPair);
            }
            return;
        }
        context.fireChannelRead(message);
    }
}

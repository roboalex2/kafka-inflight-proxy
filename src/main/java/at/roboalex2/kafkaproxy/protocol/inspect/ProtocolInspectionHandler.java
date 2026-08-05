package at.roboalex2.kafkaproxy.protocol.inspect;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import at.roboalex2.kafkaproxy.network.ConnectionPair;

/** Keeps requests transparent and routes responses through ordered inspection/transformation. */
public class ProtocolInspectionHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionProtocolContext protocolContext;
    private final TrafficDirection direction;
    private final ConnectionPair connectionPair;

    public ProtocolInspectionHandler(ConnectionProtocolContext protocolContext, TrafficDirection direction,
                                     ConnectionPair connectionPair) {
        this.protocolContext = protocolContext;
        this.direction = direction;
        this.connectionPair = connectionPair;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof ByteBuf frame) {

            if (direction == TrafficDirection.BROKER_TO_CLIENT) {
                protocolContext.processBrokerFrame(frame, outbound -> context.executor().execute(() -> {
                    if (context.channel().isActive()) {
                        context.fireChannelRead(outbound);
                    } else {
                        ReferenceCountUtil.release(outbound);
                    }
                }), connectionPair::close);
                return;
            }

            protocolContext.inspect(direction, frame);
        }
        context.fireChannelRead(message);
    }
}

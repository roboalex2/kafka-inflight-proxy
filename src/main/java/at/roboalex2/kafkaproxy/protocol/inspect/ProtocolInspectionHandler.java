package at.roboalex2.kafkaproxy.protocol.inspect;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/** Schedules inspection of a retained duplicate and passes the original frame on immediately. */
public class ProtocolInspectionHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionProtocolContext protocolContext;
    private final TrafficDirection direction;

    public ProtocolInspectionHandler(ConnectionProtocolContext protocolContext, TrafficDirection direction) {
        this.protocolContext = protocolContext;
        this.direction = direction;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof ByteBuf frame) {
            protocolContext.inspect(direction, frame);
        }
        context.fireChannelRead(message);
    }
}

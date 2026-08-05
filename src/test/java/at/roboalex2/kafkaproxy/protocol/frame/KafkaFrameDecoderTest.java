package at.roboalex2.kafkaproxy.protocol.frame;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class KafkaFrameDecoderTest {
    @Test
    void reassemblesFragmentedInputWithoutEmittingPartialFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(new KafkaFrameDecoder(1_024));
        byte[] frame = frame("fragmented".getBytes());

        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(frame, 0, 3))).isFalse();
        assertThat((Object) channel.readInbound()).isNull();
        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(frame, 3, frame.length - 3))).isTrue();

        assertFrameEquals(channel.readInbound(), frame);
        assertThat((Object) channel.readInbound()).isNull();
        channel.finishAndReleaseAll();
    }

    @Test
    void separatesCoalescedFramesAndPreservesTheirOrder() {
        EmbeddedChannel channel = new EmbeddedChannel(new KafkaFrameDecoder(1_024));
        byte[] first = frame("first".getBytes());
        byte[] second = frame("second".getBytes());

        ByteBuf coalesced = Unpooled.wrappedBuffer(first, second);
        assertThat(channel.writeInbound(coalesced)).isTrue();

        assertFrameEquals(channel.readInbound(), first);
        assertFrameEquals(channel.readInbound(), second);
        assertThat((Object) channel.readInbound()).isNull();
        channel.finishAndReleaseAll();
    }

    private void assertFrameEquals(ByteBuf actual, byte[] expected) {
        try {
            assertThat(ByteBufUtil.getBytes(actual)).isEqualTo(expected);
        } finally {
            actual.release();
        }
    }

    private byte[] frame(byte[] payload) {
        return ByteBuffer.allocate(Integer.BYTES + payload.length)
                .putInt(payload.length)
                .put(payload)
                .array();
    }
}

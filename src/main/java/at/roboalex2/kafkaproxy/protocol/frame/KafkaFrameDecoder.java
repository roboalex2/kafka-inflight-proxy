package at.roboalex2.kafkaproxy.protocol.frame;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/** Emits complete Kafka frames while retaining the four-byte length prefix. */
public class KafkaFrameDecoder extends LengthFieldBasedFrameDecoder {
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = Integer.BYTES;

    public KafkaFrameDecoder(int maxFramePayloadSizeBytes) {
        super(totalFrameSize(maxFramePayloadSizeBytes), LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH,
                0, 0, true);
    }

    private static int totalFrameSize(int maxFramePayloadSizeBytes) {
        if (maxFramePayloadSizeBytes < 1
                || maxFramePayloadSizeBytes > Integer.MAX_VALUE - LENGTH_FIELD_LENGTH) {
            throw new IllegalArgumentException("Maximum Kafka frame payload size must be between 1 and "
                    + (Integer.MAX_VALUE - LENGTH_FIELD_LENGTH));
        }
        return maxFramePayloadSizeBytes + LENGTH_FIELD_LENGTH;
    }
}

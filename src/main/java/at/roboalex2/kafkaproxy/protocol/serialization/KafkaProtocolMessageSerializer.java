package at.roboalex2.kafkaproxy.protocol.serialization;

import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.springframework.stereotype.Component;

@Component
public class KafkaProtocolMessageSerializer implements ProtocolMessageSerializer {
    @Override
    public ByteBuffer serializeResponse(ApiMessage headerData, short headerVersion,
                                        ApiMessage responseData, short apiVersion) {
        ByteBuffer header = MessageUtil.toByteBufferAccessor(headerData, headerVersion).buffer();
        ByteBuffer body = MessageUtil.toByteBufferAccessor(responseData, apiVersion).buffer();
        int payloadLength = header.remaining() + body.remaining();
        return ByteBuffer.allocate(Integer.BYTES + payloadLength)
                .putInt(payloadLength)
                .put(header.duplicate())
                .put(body.duplicate())
                .flip();
    }
}

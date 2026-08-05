package at.roboalex2.kafkaproxy.protocol.serialization;

import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiMessage;

public interface ProtocolMessageSerializer {
    ByteBuffer serializeResponse(ApiMessage headerData, short headerVersion,
                                 ApiMessage responseData, short apiVersion);
}

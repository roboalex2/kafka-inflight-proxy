package at.roboalex2.kafkaproxy.protocol.inspect;

import java.nio.ByteBuffer;

public interface KafkaResponseInspector {
    void inspectResponse(long messageNumber, ByteBuffer frameBody);
}

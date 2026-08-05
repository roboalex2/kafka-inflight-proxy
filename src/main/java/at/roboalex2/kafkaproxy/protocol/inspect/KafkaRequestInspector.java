package at.roboalex2.kafkaproxy.protocol.inspect;

import java.nio.ByteBuffer;

public interface KafkaRequestInspector {
    void inspectRequest(long messageNumber, ByteBuffer frameBody);
}

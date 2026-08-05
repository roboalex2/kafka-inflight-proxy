package at.roboalex2.kafkaproxy.keystore.model;

import java.util.Arrays;
import java.util.UUID;

public class AssignedKey implements AutoCloseable {
    private final UUID keyId;
    private final byte[] keyBytes;

    public AssignedKey(UUID keyId, byte[] keyBytes) {
        this.keyId = keyId;
        this.keyBytes = keyBytes.clone();
    }
    public UUID getKeyId() { return keyId; }
    public byte[] copyKeyBytes() { return keyBytes.clone(); }
    @Override public void close() { Arrays.fill(keyBytes, (byte) 0); }
}

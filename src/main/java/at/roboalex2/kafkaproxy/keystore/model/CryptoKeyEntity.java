package at.roboalex2.kafkaproxy.keystore.model;

import java.util.UUID;

public class CryptoKeyEntity {
    private final UUID keyId;
    private final String wrappedKeyEnvelope;

    public CryptoKeyEntity(UUID keyId, String wrappedKeyEnvelope) {
        this.keyId = keyId;
        this.wrappedKeyEnvelope = wrappedKeyEnvelope;
    }

    public UUID getKeyId() { return keyId; }
    public String getWrappedKeyEnvelope() { return wrappedKeyEnvelope; }
}

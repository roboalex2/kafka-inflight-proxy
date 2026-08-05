package at.roboalex2.kafkaproxy.crypto.envelope;

public class AesGcmEnvelope {
    private final byte version;
    private final byte[] nonce;
    private final byte[] ciphertextAndTag;

    public AesGcmEnvelope(byte version, byte[] nonce, byte[] ciphertextAndTag) {
        this.version = version;
        this.nonce = nonce.clone();
        this.ciphertextAndTag = ciphertextAndTag.clone();
    }

    public byte getVersion() { return version; }
    public byte[] copyNonce() { return nonce.clone(); }
    public byte[] copyCiphertextAndTag() { return ciphertextAndTag.clone(); }
}

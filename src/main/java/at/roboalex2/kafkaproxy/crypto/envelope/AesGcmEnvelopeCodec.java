package at.roboalex2.kafkaproxy.crypto.envelope;

import at.roboalex2.kafkaproxy.crypto.exception.CryptographicOperationException;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.springframework.stereotype.Component;

@Component
public class AesGcmEnvelopeCodec {
    public static final byte CURRENT_VERSION = 1;
    public static final int NONCE_LENGTH_BYTES = 12;
    private static final int MINIMUM_CIPHERTEXT_AND_TAG_BYTES = 16;

    public String encode(AesGcmEnvelope envelope) {
        byte[] nonce = envelope.copyNonce();
        byte[] ciphertext = envelope.copyCiphertextAndTag();
        validate(envelope.getVersion(), nonce, ciphertext);
        return Base64.getEncoder().encodeToString(ByteBuffer.allocate(1 + nonce.length + ciphertext.length)
                .put(envelope.getVersion()).put(nonce).put(ciphertext).array());
    }

    public AesGcmEnvelope decode(String encoded) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            if (bytes.length < 1 + NONCE_LENGTH_BYTES + MINIMUM_CIPHERTEXT_AND_TAG_BYTES) {
                throw new CryptographicOperationException("Wrapped key envelope is malformed");
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            byte version = buffer.get();
            byte[] nonce = new byte[NONCE_LENGTH_BYTES];
            buffer.get(nonce);
            byte[] ciphertext = new byte[buffer.remaining()];
            buffer.get(ciphertext);
            validate(version, nonce, ciphertext);
            return new AesGcmEnvelope(version, nonce, ciphertext);
        } catch (IllegalArgumentException exception) {
            throw new CryptographicOperationException("Wrapped key envelope is malformed", exception);
        }
    }

    private void validate(byte version, byte[] nonce, byte[] ciphertext) {
        if (version != CURRENT_VERSION) {
            throw new CryptographicOperationException("Unsupported wrapped key envelope version");
        }
        if (nonce.length != NONCE_LENGTH_BYTES || ciphertext.length < MINIMUM_CIPHERTEXT_AND_TAG_BYTES) {
            throw new CryptographicOperationException("Wrapped key envelope is malformed");
        }
    }
}

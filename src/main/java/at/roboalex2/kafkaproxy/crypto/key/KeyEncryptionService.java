package at.roboalex2.kafkaproxy.crypto.key;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelope;
import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelopeCodec;
import at.roboalex2.kafkaproxy.crypto.exception.CryptographicOperationException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KeyEncryptionService {
    private static final int AUTHENTICATION_TAG_BITS = 128;
    private final SecretKeySpec keyEncryptionKey;
    private final SecureRandom secureRandom;
    private final AesGcmEnvelopeCodec envelopeCodec;

    @Autowired
    public KeyEncryptionService(KafkaProxyProperties properties, AesGcmEnvelopeCodec envelopeCodec) {
        this(properties.getCrypto().getKeyEncryptionKey(), envelopeCodec, new SecureRandom());
    }

    KeyEncryptionService(String secret, AesGcmEnvelopeCodec envelopeCodec, SecureRandom secureRandom) {
        this.keyEncryptionKey = new SecretKeySpec(derive(secret), "AES");
        this.envelopeCodec = envelopeCodec;
        this.secureRandom = secureRandom;
    }

    public String wrap(UUID keyId, byte[] dataEncryptionKey) {
        if (dataEncryptionKey == null || dataEncryptionKey.length != DataEncryptionKeyGenerator.KEY_LENGTH_BYTES) {
            throw new CryptographicOperationException("Data-encryption key must be exactly 32 bytes");
        }
        byte[] nonce = new byte[AesGcmEnvelopeCodec.NONCE_LENGTH_BYTES];
        secureRandom.nextBytes(nonce);
        try {
            Cipher cipher = cipher(Cipher.ENCRYPT_MODE, keyId, nonce);
            return envelopeCodec.encode(new AesGcmEnvelope(AesGcmEnvelopeCodec.CURRENT_VERSION, nonce,
                    cipher.doFinal(dataEncryptionKey)));
        } catch (GeneralSecurityException exception) {
            throw failure(exception);
        }
    }

    public byte[] unwrap(UUID keyId, String wrappedEnvelope) {
        AesGcmEnvelope envelope = envelopeCodec.decode(wrappedEnvelope);
        try {
            Cipher cipher = cipher(Cipher.DECRYPT_MODE, keyId, envelope.copyNonce());
            byte[] key = cipher.doFinal(envelope.copyCiphertextAndTag());
            if (key.length != DataEncryptionKeyGenerator.KEY_LENGTH_BYTES) {
                throw new CryptographicOperationException("Unwrapped data-encryption key has an invalid length");
            }
            return key;
        } catch (GeneralSecurityException exception) {
            throw failure(exception);
        }
    }

    public int derivedKeyLengthBytes() { return keyEncryptionKey.getEncoded().length; }

    private Cipher cipher(int mode, UUID keyId, byte[] nonce) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, keyEncryptionKey, new GCMParameterSpec(AUTHENTICATION_TAG_BITS, nonce));
        cipher.updateAAD(keyId.toString().getBytes(StandardCharsets.UTF_8));
        return cipher;
    }

    private byte[] derive(String secret) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private CryptographicOperationException failure(Exception cause) {
        return new CryptographicOperationException("Cryptographic key operation failed", cause);
    }
}

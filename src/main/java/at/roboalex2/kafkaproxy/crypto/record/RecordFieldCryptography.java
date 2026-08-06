package at.roboalex2.kafkaproxy.crypto.record;

import at.roboalex2.kafkaproxy.crypto.exception.CryptographicOperationException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Service;

@Service
public class RecordFieldCryptography {
    public static final int IV_BYTES = 12;
    private static final int AUTHENTICATION_TAG_BITS = 128;
    private static final int AUTHENTICATION_TAG_BYTES = 16;
    private static final String ENCRYPTED_HEADER_PREFIX = "enc:";
    private final SecureRandom secureRandom = new SecureRandom();

    public byte[] newIv() {
        byte[] iv = new byte[IV_BYTES];
        secureRandom.nextBytes(iv);
        return iv;
    }

    public byte[] encrypt(byte[] plaintext, byte[] dek, byte[] iv, UUID keyId) {
        validateInputs(dek, iv);
        try {
            Cipher cipher = cipher(Cipher.ENCRYPT_MODE, dek, iv);
            cipher.updateAAD(keyId.toString().getBytes(StandardCharsets.UTF_8));
            return cipher.doFinal(plaintext);
        } catch (GeneralSecurityException exception) {
            throw failure(exception);
        }
    }

    public byte[] decrypt(byte[] ciphertext, byte[] dek, byte[] iv, UUID keyId) {
        validateInputs(dek, iv);
        if (ciphertext == null || ciphertext.length < AUTHENTICATION_TAG_BYTES) {
            throw new CryptographicOperationException("Encrypted record field is malformed");
        }
        try {
            Cipher cipher = cipher(Cipher.DECRYPT_MODE, dek, iv);
            cipher.updateAAD(keyId.toString().getBytes(StandardCharsets.UTF_8));
            return cipher.doFinal(ciphertext);
        } catch (GeneralSecurityException exception) {
            throw failure(exception);
        }
    }

    public String encryptHeaderKey(String key, byte[] dek, byte[] iv, UUID keyId) {
        byte[] encrypted = encrypt(key.getBytes(StandardCharsets.UTF_8), dek, iv, keyId);
        return ENCRYPTED_HEADER_PREFIX + Base64.getEncoder().encodeToString(encrypted);
    }

    public String decryptHeaderKey(String key, byte[] dek, byte[] iv, UUID keyId) {
        if (key == null || !key.startsWith(ENCRYPTED_HEADER_PREFIX)) {
            throw new CryptographicOperationException("Encrypted Kafka header key is malformed");
        }
        try {
            String encoded = key.substring(ENCRYPTED_HEADER_PREFIX.length());
            byte[] ciphertext = Base64.getDecoder().decode(encoded);
            return new String(decrypt(ciphertext, dek, iv, keyId), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new CryptographicOperationException("Encrypted Kafka header key is malformed", exception);
        }
    }

    private Cipher cipher(int mode, byte[] dek, byte[] iv) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(AUTHENTICATION_TAG_BITS, iv));
        return cipher;
    }

    private void validateInputs(byte[] dek, byte[] iv) {
        if (dek == null || dek.length != 32) {
            throw new CryptographicOperationException("Data-encryption key must be exactly 32 bytes");
        }
        if (iv == null || iv.length != IV_BYTES) {
            throw new CryptographicOperationException("Record IV must be exactly 12 bytes");
        }
    }

    private CryptographicOperationException failure(Exception cause) {
        return new CryptographicOperationException("Record-field cryptographic operation failed", cause);
    }
}

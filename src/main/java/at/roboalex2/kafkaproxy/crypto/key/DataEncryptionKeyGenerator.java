package at.roboalex2.kafkaproxy.crypto.key;

import java.security.SecureRandom;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class DataEncryptionKeyGenerator {
    public static final int KEY_LENGTH_BYTES = 32;
    private final SecureRandom secureRandom;

    public DataEncryptionKeyGenerator() { this(new SecureRandom()); }
    DataEncryptionKeyGenerator(SecureRandom secureRandom) { this.secureRandom = secureRandom; }

    public KeyMaterial generate() {
        byte[] key = new byte[KEY_LENGTH_BYTES];
        secureRandom.nextBytes(key);
        return new KeyMaterial(UUID.randomUUID(), key);
    }
}

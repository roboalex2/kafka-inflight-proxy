package at.roboalex2.kafkaproxy.crypto.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelopeCodec;
import at.roboalex2.kafkaproxy.crypto.exception.CryptographicOperationException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class KeyEncryptionServiceTest {
    private final AesGcmEnvelopeCodec codec = new AesGcmEnvelopeCodec();

    @Test
    void generatesIndependent256BitKeys() {
        DataEncryptionKeyGenerator generator = new DataEncryptionKeyGenerator();
        try (KeyMaterial first = generator.generate(); KeyMaterial second = generator.generate()) {
            assertThat(first.copyKeyBytes()).hasSize(32).isNotEqualTo(second.copyKeyBytes());
            assertThat(first.getKeyId()).isNotEqualTo(second.getKeyId());
        }
    }

    @Test
    void wrapsAndUnwrapsWithFreshNonceAnd256BitDerivedKek() {
        KeyEncryptionService service = service("operator-secret");
        UUID keyId = UUID.randomUUID();
        byte[] plaintext = new byte[32];
        new SecureRandom().nextBytes(plaintext);

        String first = service.wrap(keyId, plaintext);
        String second = service.wrap(keyId, plaintext);

        assertThat(service.derivedKeyLengthBytes()).isEqualTo(32);
        assertThat(first).isNotEqualTo(second);
        assertThat(service.unwrap(keyId, first)).isEqualTo(plaintext);
        assertThat(Base64.getDecoder().decode(first)[0]).isEqualTo((byte) 1);
    }

    @Test
    void rejectsWrongKekWrongKeyIdTamperingAndUnknownEnvelopeVersion() {
        UUID keyId = UUID.randomUUID();
        String wrapped = service("right-secret").wrap(keyId, new byte[32]);
        assertThatThrownBy(() -> service("wrong-secret").unwrap(keyId, wrapped))
                .isInstanceOf(CryptographicOperationException.class);
        assertThatThrownBy(() -> service("right-secret").unwrap(UUID.randomUUID(), wrapped))
                .isInstanceOf(CryptographicOperationException.class);

        byte[] tampered = Base64.getDecoder().decode(wrapped);
        tampered[tampered.length - 1] ^= 1;
        assertThatThrownBy(() -> service("right-secret").unwrap(keyId,
                Base64.getEncoder().encodeToString(tampered)))
                .isInstanceOf(CryptographicOperationException.class);

        byte[] unknownVersion = Base64.getDecoder().decode(wrapped);
        unknownVersion[0] = 2;
        assertThatThrownBy(() -> service("right-secret").unwrap(keyId,
                Base64.getEncoder().encodeToString(unknownVersion)))
                .isInstanceOf(CryptographicOperationException.class);
    }

    private KeyEncryptionService service(String secret) {
        return new KeyEncryptionService(secret, codec, new SecureRandom());
    }
}

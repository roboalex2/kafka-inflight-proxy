package at.roboalex2.kafkaproxy.crypto.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.crypto.exception.CryptographicOperationException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecordFieldCryptographyTest {
    private final RecordFieldCryptography cryptography = new RecordFieldCryptography();
    private final byte[] dek = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private final byte[] iv = "0123456789ab".getBytes(StandardCharsets.UTF_8);
    private final UUID keyId = UUID.randomUUID();

    @Test
    void roundTripsBinaryFieldsAndBase64HeaderKeys() {
        byte[] encrypted = cryptography.encrypt(new byte[0], dek, iv, keyId);
        assertThat(encrypted).hasSize(16);
        assertThat(cryptography.decrypt(encrypted, dek, iv, keyId)).isEmpty();

        String header = cryptography.encryptHeaderKey("trace-header", dek, iv, keyId);
        assertThat(header).startsWith("enc:");
        assertThat(cryptography.decryptHeaderKey(header, dek, iv, keyId)).isEqualTo("trace-header");
    }

    @Test
    void allFieldsUseTheSameIvAndOnlyKeyIdIsAuthenticated() {
        byte[] plaintext = "same-input".getBytes(StandardCharsets.UTF_8);
        byte[] first = cryptography.encrypt(plaintext, dek, iv, keyId);
        byte[] second = cryptography.encrypt(plaintext, dek, iv, keyId);
        assertThat(first).isEqualTo(second);
        assertThatThrownBy(() -> cryptography.decrypt(first, dek, iv, UUID.randomUUID()))
                .isInstanceOf(CryptographicOperationException.class);
    }

    @Test
    void createsFreshTwelveByteRecordIvs() {
        byte[] first = cryptography.newIv();
        byte[] second = cryptography.newIv();
        assertThat(first).hasSize(12).isNotEqualTo(second);
    }
}

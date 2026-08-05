package at.roboalex2.kafkaproxy.crypto.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RecordKeyHashServiceTest {
    private final RecordKeyHashService service = new RecordKeyHashService();

    @Test
    void hashesKeysWithLowercaseSha256AndTreatsNullAsEmpty() {
        assertThat(service.hash("abc".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assertThat(service.hash(null)).isEqualTo(service.hash(new byte[0]))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void normalizesUppercaseRestHashesAndRejectsMalformedValues() {
        String uppercase = "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD";
        assertThat(service.normalizeAndValidate(uppercase)).isEqualTo(uppercase.toLowerCase());
        assertThatThrownBy(() -> service.normalizeAndValidate("not-a-hash"))
                .isInstanceOfSatisfying(BackendException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BackendErrorCode.INVALID_RECORD_KEY_HASH));
    }
}

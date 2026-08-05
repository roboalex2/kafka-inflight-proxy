package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

public class CryptoProperties {
    public static final String ENCRYPTION_KEY_HEADER = "encryption-key";
    public static final String ENCRYPTION_IV_HEADER = "encryption-iv";

    @NotBlank(message = "must not be blank")
    private String keyEncryptionKey;
    @NotBlank(message = "must not be blank")
    private String encryptionKeyHeaderName = ENCRYPTION_KEY_HEADER;
    @NotBlank(message = "must not be blank")
    private String encryptionIvHeaderName = ENCRYPTION_IV_HEADER;

    public String getKeyEncryptionKey() { return keyEncryptionKey; }
    public void setKeyEncryptionKey(String keyEncryptionKey) { this.keyEncryptionKey = keyEncryptionKey; }
    public String getEncryptionKeyHeaderName() { return encryptionKeyHeaderName; }
    public void setEncryptionKeyHeaderName(String encryptionKeyHeaderName) {
        this.encryptionKeyHeaderName = encryptionKeyHeaderName;
    }
    public String getEncryptionIvHeaderName() { return encryptionIvHeaderName; }
    public void setEncryptionIvHeaderName(String encryptionIvHeaderName) {
        this.encryptionIvHeaderName = encryptionIvHeaderName;
    }

    @AssertTrue(message = "encryption-key-header-name must equal encryption-key")
    public boolean isEncryptionKeyHeaderNameValid() {
        return ENCRYPTION_KEY_HEADER.equals(encryptionKeyHeaderName);
    }

    @AssertTrue(message = "encryption-iv-header-name must equal encryption-iv")
    public boolean isEncryptionIvHeaderNameValid() {
        return ENCRYPTION_IV_HEADER.equals(encryptionIvHeaderName);
    }
}

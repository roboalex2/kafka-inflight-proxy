package at.roboalex2.kafkaproxy.crypto.hash;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class RecordKeyHashService {
    private static final Pattern SHA_256_HEX = Pattern.compile("[0-9a-fA-F]{64}");

    public String hash(byte[] recordKey) {
        byte[] input = recordKey == null ? new byte[0] : recordKey;
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    public String normalizeAndValidate(String hash) {
        if (hash == null || !SHA_256_HEX.matcher(hash).matches()) {
            throw new BackendServiceException(BackendErrorCode.INVALID_RECORD_KEY_HASH,
                    "Record-key hash must contain exactly 64 hexadecimal characters");
        }
        return hash.toLowerCase(Locale.ROOT);
    }
}

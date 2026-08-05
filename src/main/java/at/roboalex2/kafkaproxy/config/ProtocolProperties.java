package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class ProtocolProperties {
    public static final int DEFAULT_MAX_FRAME_SIZE_BYTES = 100 * 1024 * 1024;
    @Min(value = 1, message = "must be at least 1 byte")
    @Max(value = Integer.MAX_VALUE, message = "must fit in a signed 32-bit Kafka frame length")
    private int maxFrameSizeBytes = DEFAULT_MAX_FRAME_SIZE_BYTES;

    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public void setMaxFrameSizeBytes(int maxFrameSizeBytes) {
        this.maxFrameSizeBytes = maxFrameSizeBytes;
    }
}

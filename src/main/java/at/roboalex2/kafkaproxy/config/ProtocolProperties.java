package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class ProtocolProperties {
    public static final int DEFAULT_MAX_FRAME_SIZE_BYTES = 100 * 1024 * 1024;
    public static final int MAX_FRAME_PAYLOAD_SIZE_BYTES = Integer.MAX_VALUE - Integer.BYTES;

    @Min(value = 1, message = "must be at least 1 byte")
    @Max(value = MAX_FRAME_PAYLOAD_SIZE_BYTES,
            message = "must leave room for the 4-byte Kafka frame length prefix")
    private int maxFrameSizeBytes = DEFAULT_MAX_FRAME_SIZE_BYTES;
    @Min(value = 1, message = "must be at least 1 worker")
    private int transformationWorkerThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    @Min(value = 1, message = "must be at least 1 task")
    private int transformationExecutorQueueCapacity = 1024;
    @Min(value = 1, message = "must be at least 1 frame")
    private int perConnectionTransformationQueueCapacity = 64;

    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public void setMaxFrameSizeBytes(int maxFrameSizeBytes) {
        this.maxFrameSizeBytes = maxFrameSizeBytes;
    }

    public int getTransformationWorkerThreads() { return transformationWorkerThreads; }
    public void setTransformationWorkerThreads(int value) { this.transformationWorkerThreads = value; }
    public int getTransformationExecutorQueueCapacity() { return transformationExecutorQueueCapacity; }
    public void setTransformationExecutorQueueCapacity(int value) {
        this.transformationExecutorQueueCapacity = value;
    }
    public int getPerConnectionTransformationQueueCapacity() {
        return perConnectionTransformationQueueCapacity;
    }
    public void setPerConnectionTransformationQueueCapacity(int value) {
        this.perConnectionTransformationQueueCapacity = value;
    }
}

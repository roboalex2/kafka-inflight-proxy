package at.roboalex2.kafkaproxy.protocol.model;

import java.util.List;

public class RecordBatchModel {
    private final long baseOffset;
    private final int batchLength;
    private final int partitionLeaderEpoch;
    private final byte magic;
    private final long crc;
    private final short attributes;
    private final String compressionType;
    private final String timestampType;
    private final boolean transactional;
    private final boolean controlBatch;
    private final boolean deleteHorizon;
    private final int lastOffsetDelta;
    private final long baseTimestamp;
    private final long maxTimestamp;
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;
    private final int recordCount;
    private final List<RecordModel> records;

    public RecordBatchModel(long baseOffset, int batchLength, int partitionLeaderEpoch, byte magic,
                            long crc, short attributes, String compressionType, String timestampType,
                            boolean transactional, boolean controlBatch, boolean deleteHorizon,
                            int lastOffsetDelta, long baseTimestamp, long maxTimestamp, long producerId,
                            short producerEpoch, int baseSequence, int recordCount, List<RecordModel> records) {
        this.baseOffset = baseOffset;
        this.batchLength = batchLength;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.magic = magic;
        this.crc = crc;
        this.attributes = attributes;
        this.compressionType = compressionType;
        this.timestampType = timestampType;
        this.transactional = transactional;
        this.controlBatch = controlBatch;
        this.deleteHorizon = deleteHorizon;
        this.lastOffsetDelta = lastOffsetDelta;
        this.baseTimestamp = baseTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.recordCount = recordCount;
        this.records = List.copyOf(records);
    }

    public long getBaseOffset() { return baseOffset; }
    public int getBatchLength() { return batchLength; }
    public int getPartitionLeaderEpoch() { return partitionLeaderEpoch; }
    public byte getMagic() { return magic; }
    public long getCrc() { return crc; }
    public short getAttributes() { return attributes; }
    public String getCompressionType() { return compressionType; }
    public String getTimestampType() { return timestampType; }
    public boolean isTransactional() { return transactional; }
    public boolean isControlBatch() { return controlBatch; }
    public boolean isDeleteHorizon() { return deleteHorizon; }
    public int getLastOffsetDelta() { return lastOffsetDelta; }
    public long getBaseTimestamp() { return baseTimestamp; }
    public long getMaxTimestamp() { return maxTimestamp; }
    public long getProducerId() { return producerId; }
    public short getProducerEpoch() { return producerEpoch; }
    public int getBaseSequence() { return baseSequence; }
    public int getRecordCount() { return recordCount; }
    public List<RecordModel> getRecords() { return records; }
}

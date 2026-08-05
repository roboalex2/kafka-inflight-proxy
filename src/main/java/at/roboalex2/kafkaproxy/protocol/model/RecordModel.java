package at.roboalex2.kafkaproxy.protocol.model;

import java.util.List;

public class RecordModel {
    private final int encodedLength;
    private final byte attributes;
    private final long timestampDelta;
    private final long offsetDelta;
    private final int keyLength;
    private final byte[] key;
    private final int valueLength;
    private final byte[] value;
    private final int headerCount;
    private final List<RecordHeaderModel> headers;

    public RecordModel(int encodedLength, byte attributes, long timestampDelta, long offsetDelta,
                       int keyLength, byte[] key, int valueLength, byte[] value,
                       List<RecordHeaderModel> headers) {
        this.encodedLength = encodedLength;
        this.attributes = attributes;
        this.timestampDelta = timestampDelta;
        this.offsetDelta = offsetDelta;
        this.keyLength = keyLength;
        this.key = key;
        this.valueLength = valueLength;
        this.value = value;
        this.headerCount = headers.size();
        this.headers = List.copyOf(headers);
    }

    public int getEncodedLength() { return encodedLength; }
    public byte getAttributes() { return attributes; }
    public long getTimestampDelta() { return timestampDelta; }
    public long getOffsetDelta() { return offsetDelta; }
    public int getKeyLength() { return keyLength; }
    public byte[] getKey() { return key; }
    public int getValueLength() { return valueLength; }
    public byte[] getValue() { return value; }
    public int getHeaderCount() { return headerCount; }
    public List<RecordHeaderModel> getHeaders() { return headers; }
}

package at.roboalex2.kafkaproxy.protocol.model;

public class RecordHeaderModel {
    private final int keyLength;
    private final String key;
    private final int valueLength;
    private final byte[] value;

    public RecordHeaderModel(int keyLength, String key, int valueLength, byte[] value) {
        this.keyLength = keyLength;
        this.key = key;
        this.valueLength = valueLength;
        this.value = value;
    }

    public int getKeyLength() { return keyLength; }
    public String getKey() { return key; }
    public int getValueLength() { return valueLength; }
    public byte[] getValue() { return value; }
}

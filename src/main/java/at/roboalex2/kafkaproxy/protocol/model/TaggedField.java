package at.roboalex2.kafkaproxy.protocol.model;

public class TaggedField {
    private final int tag;
    private final byte[] value;

    public TaggedField(int tag, byte[] value) {
        this.tag = tag;
        this.value = value;
    }

    public int getTag() { return tag; }
    public byte[] getValue() { return value; }
}

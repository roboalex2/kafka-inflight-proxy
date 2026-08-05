package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class MetadataRequestModel extends ProtocolMessageModel {
    public MetadataRequestModel(short version, Map<String, Object> fields) { super(version, fields); }
}

package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class MetadataResponseModel extends ProtocolMessageModel {
    public MetadataResponseModel(short version, Map<String, Object> fields) { super(version, fields); }
}

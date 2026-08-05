package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class FetchRequestModel extends ProtocolMessageModel {
    public FetchRequestModel(short version, Map<String, Object> fields) { super(version, fields); }
}

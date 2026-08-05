package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class ProduceRequestModel extends ProtocolMessageModel {
    public ProduceRequestModel(short version, Map<String, Object> fields) { super(version, fields); }
}

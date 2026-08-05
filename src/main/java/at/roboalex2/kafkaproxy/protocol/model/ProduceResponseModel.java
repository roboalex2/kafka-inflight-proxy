package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class ProduceResponseModel extends ProtocolMessageModel {
    public ProduceResponseModel(short version, Map<String, Object> fields) { super(version, fields); }
}

package at.roboalex2.kafkaproxy.protocol.model;
import java.util.Map;
public class FetchResponseModel extends ProtocolMessageModel {
    public FetchResponseModel(short version, Map<String, Object> fields) { super(version, fields); }
}

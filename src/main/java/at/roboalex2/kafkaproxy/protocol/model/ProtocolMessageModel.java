package at.roboalex2.kafkaproxy.protocol.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class ProtocolMessageModel {
    private final short apiVersion;
    private final Map<String, Object> fields;

    protected ProtocolMessageModel(short apiVersion, Map<String, Object> fields) {
        this.apiVersion = apiVersion;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public short getApiVersion() { return apiVersion; }
    public Map<String, Object> getFields() { return fields; }
}

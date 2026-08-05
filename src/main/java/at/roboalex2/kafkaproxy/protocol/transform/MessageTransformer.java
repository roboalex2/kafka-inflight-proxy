package at.roboalex2.kafkaproxy.protocol.transform;

import org.apache.kafka.common.protocol.ApiMessage;

public interface MessageTransformer {
    ApiMessage transform(short apiKey, short apiVersion, ApiMessage message);
}

package at.roboalex2.kafkaproxy.protocol.transform;

import org.apache.kafka.common.protocol.ApiMessage;

public record MessageTransformationResult(ApiMessage message, boolean changed) { }

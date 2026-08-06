package at.roboalex2.kafkaproxy.protocol.transform;

import org.apache.kafka.common.header.Header;

public record RecordComponents(byte[] key, byte[] value, Header[] headers, boolean changed) { }

package at.roboalex2.kafkaproxy.protocol.transform;

import org.apache.kafka.common.record.MemoryRecords;

public record RecordsTransformationResult(MemoryRecords records, boolean changed) { }

package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolCodecRegistry;
import io.netty.channel.Channel;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ConnectionProtocolContextFactory {
    private final ProtocolCodecRegistry codecs;
    private final ConnectionLogWriterFactory logWriters;
    private final ObjectMapper objectMapper;
    private final ProtocolInspectionExecutor executor;

    public ConnectionProtocolContextFactory(ProtocolCodecRegistry codecs, ConnectionLogWriterFactory logWriters,
                                            ObjectMapper objectMapper, ProtocolInspectionExecutor executor) {
        this.codecs = codecs;
        this.logWriters = logWriters;
        this.objectMapper = objectMapper;
        this.executor = executor;
    }

    public ConnectionProtocolContext create(Channel clientChannel) {
        return new ConnectionProtocolContext(clientChannel.id().asLongText(), codecs,
                logWriters.create(clientChannel), objectMapper, executor);
    }
}

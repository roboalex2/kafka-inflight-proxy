package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import io.netty.channel.Channel;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ConnectionProtocolContextFactory {
    private final ProtocolParser protocolParser;
    private final ConnectionLogWriterFactory logWriters;
    private final ObjectMapper objectMapper;
    private final VirtualThreadExecutor executor;

    public ConnectionProtocolContextFactory(ProtocolParser protocolParser, ConnectionLogWriterFactory logWriters,
                                            ObjectMapper objectMapper, VirtualThreadExecutor executor) {
        this.protocolParser = protocolParser;
        this.logWriters = logWriters;
        this.objectMapper = objectMapper;
        this.executor = executor;
    }

    public ConnectionProtocolContext create(Channel clientChannel) {
        return new ConnectionProtocolContext(clientChannel.id().asLongText(), protocolParser,
                logWriters.create(clientChannel), objectMapper, executor);
    }
}

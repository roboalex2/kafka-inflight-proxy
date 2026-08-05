package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.ProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.transform.MessageTransformer;
import io.netty.channel.Channel;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ConnectionProtocolContextFactory {
    private final ProtocolParser protocolParser;
    private final ConnectionLogWriterFactory logWriters;
    private final ObjectMapper objectMapper;
    private final VirtualThreadExecutor executor;
    private final MessageTransformer transformer;
    private final ProtocolMessageSerializer serializer;
    private final ProtocolModelMapper modelMapper;

    public ConnectionProtocolContextFactory(ProtocolParser protocolParser, ConnectionLogWriterFactory logWriters,
                                            ObjectMapper objectMapper, VirtualThreadExecutor executor,
                                            MessageTransformer transformer, ProtocolMessageSerializer serializer,
                                            ProtocolModelMapper modelMapper) {
        this.protocolParser = protocolParser;
        this.logWriters = logWriters;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.transformer = transformer;
        this.serializer = serializer;
        this.modelMapper = modelMapper;
    }

    public ConnectionProtocolContext create(Channel clientChannel) {
        return new ConnectionProtocolContext(clientChannel.id().asLongText(), protocolParser,
                logWriters.create(clientChannel), objectMapper, executor, transformer, serializer, modelMapper);
    }
}

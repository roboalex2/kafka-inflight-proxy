package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriterFactory;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.KafkaProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.topic.TopicIdentityResolver;
import at.roboalex2.kafkaproxy.protocol.transform.FetchResponseDecryptionTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataEndpointTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.ProduceRequestEncryptionTransformer;
import io.netty.channel.Channel;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ConnectionProtocolContextFactory {
    private final ProtocolParser protocolParser;
    private final ConnectionLogWriterFactory logWriters;
    private final ObjectMapper objectMapper;
    private final TransformationExecutor executor;
    private final MetadataEndpointTransformer metadataTransformer;
    private final ProduceRequestEncryptionTransformer produceTransformer;
    private final FetchResponseDecryptionTransformer fetchTransformer;
    private final TopicIdentityResolver topicIdentityResolver;
    private final KafkaProtocolMessageSerializer serializer;
    private final ProtocolModelMapper modelMapper;
    private final int perConnectionQueueCapacity;

    public ConnectionProtocolContextFactory(ProtocolParser protocolParser, ConnectionLogWriterFactory logWriters,
                                            ObjectMapper objectMapper, TransformationExecutor executor,
                                            MetadataEndpointTransformer metadataTransformer,
                                            ProduceRequestEncryptionTransformer produceTransformer,
                                            FetchResponseDecryptionTransformer fetchTransformer,
                                            TopicIdentityResolver topicIdentityResolver,
                                            KafkaProtocolMessageSerializer serializer, ProtocolModelMapper modelMapper,
                                            KafkaProxyProperties properties) {
        this.protocolParser = protocolParser;
        this.logWriters = logWriters;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.metadataTransformer = metadataTransformer;
        this.produceTransformer = produceTransformer;
        this.fetchTransformer = fetchTransformer;
        this.topicIdentityResolver = topicIdentityResolver;
        this.serializer = serializer;
        this.modelMapper = modelMapper;
        this.perConnectionQueueCapacity = properties.getProtocol()
                .getPerConnectionTransformationQueueCapacity();
    }

    public ConnectionProtocolContext create(Channel clientChannel) {
        return new ConnectionProtocolContext(clientChannel.id().asLongText(), protocolParser,
                logWriters.create(clientChannel), objectMapper, executor, perConnectionQueueCapacity,
                metadataTransformer, produceTransformer,
                fetchTransformer, topicIdentityResolver, serializer, modelMapper);
    }
}

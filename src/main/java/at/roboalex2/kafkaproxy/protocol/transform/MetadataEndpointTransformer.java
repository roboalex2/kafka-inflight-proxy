package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.springframework.stereotype.Component;

/** Rewrites only Metadata broker host/port fields after validating the entire topology. */
@Component
public class MetadataEndpointTransformer implements MessageTransformer {
    private final Map<Endpoint, Endpoint> brokerProxyAddresses;

    public MetadataEndpointTransformer(KafkaProxyProperties properties) {
        this.brokerProxyAddresses = Map.copyOf(new LinkedHashMap<>(properties.getBrokerProxyAddresses()));
    }

    @Override
    public ApiMessage transform(short apiKey, short apiVersion, ApiMessage message) {
        if (apiKey != ApiKeys.METADATA.id || apiVersion < 0 || apiVersion > 13
                || !(message instanceof MetadataResponseData metadata)) {
            return message;
        }

        // Validate every endpoint before changing one, preventing a partially proxied topology.
        for (MetadataResponseData.MetadataResponseBroker broker : metadata.brokers()) {
            Endpoint advertised = new Endpoint(broker.host(), broker.port());
            if (!brokerProxyAddresses.containsKey(advertised)) {
                throw new MissingBrokerMappingException(advertised);
            }
        }
        for (MetadataResponseData.MetadataResponseBroker broker : metadata.brokers()) {
            Endpoint proxy = brokerProxyAddresses.get(new Endpoint(broker.host(), broker.port()));
            broker.setHost(proxy.getHost());
            broker.setPort(proxy.getPort());
        }
        return metadata;
    }
}

package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.message.MetadataResponseData;
import org.springframework.stereotype.Component;

/** Rewrites only Metadata broker host/port fields after validating the entire topology. */
@Component
public class MetadataEndpointTransformer {
    private final Map<Endpoint, Endpoint> brokerProxyAddresses;

    public MetadataEndpointTransformer(KafkaProxyProperties properties) {
        this.brokerProxyAddresses = Map.copyOf(new LinkedHashMap<>(properties.getBrokerProxyAddresses()));
    }

    public MetadataResponseData transform(MetadataResponseData metadata, short apiVersion) {
        if (apiVersion < 0 || apiVersion > 13) return metadata;
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

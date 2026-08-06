package at.roboalex2.kafkaproxy.protocol.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.MetadataResponseData;
import org.junit.jupiter.api.Test;

class MetadataEndpointTransformerTest {
    @Test
    void rewritesEveryBrokerAndPreservesAllOtherMetadata() {
        MetadataResponseData response = response();
        MetadataResponseData original = response.duplicate();
        MetadataEndpointTransformer transformer = transformer(Map.of(
                new Endpoint("broker-a", 9092), new Endpoint("proxy-a", 19092),
                new Endpoint("broker-b", 9093), new Endpoint("proxy-b", 19093)));

        transformer.transform(response, (short) 13);

        assertThat(response.brokers().find(1).host()).isEqualTo("proxy-a");
        assertThat(response.brokers().find(1).port()).isEqualTo(19092);
        assertThat(response.brokers().find(1).nodeId()).isEqualTo(1);
        assertThat(response.brokers().find(1).rack()).isEqualTo("rack-a");
        assertThat(response.brokers().find(2).host()).isEqualTo("proxy-b");
        assertThat(response.brokers().find(2).port()).isEqualTo(19093);
        assertThat(response.topics()).isEqualTo(original.topics());
        assertThat(response.clusterId()).isEqualTo(original.clusterId());
        assertThat(response.controllerId()).isEqualTo(original.controllerId());
        assertThat(response.throttleTimeMs()).isEqualTo(original.throttleTimeMs());
        assertThat(response.unknownTaggedFields()).isEqualTo(original.unknownTaggedFields());
    }

    @Test
    void validatesTheWholeTopologyBeforeChangingAnyBroker() {
        MetadataResponseData response = response();
        MetadataEndpointTransformer transformer = transformer(Map.of(
                new Endpoint("broker-a", 9092), new Endpoint("proxy-a", 19092)));

        assertThatThrownBy(() -> transformer.transform(response, (short) 13))
                .isInstanceOf(MissingBrokerMappingException.class)
                .hasMessageContaining("broker-b:9093");
        assertThat(response.brokers().find(1).host()).isEqualTo("broker-a");
        assertThat(response.brokers().find(1).port()).isEqualTo(9092);
        assertThat(response.brokers().find(2).host()).isEqualTo("broker-b");
        assertThat(response.brokers().find(2).port()).isEqualTo(9093);
    }

    private MetadataEndpointTransformer transformer(Map<Endpoint, Endpoint> mappings) {
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.setBrokerProxyAddresses(new LinkedHashMap<>(mappings));
        return new MetadataEndpointTransformer(properties);
    }

    static MetadataResponseData response() {
        MetadataResponseData response = new MetadataResponseData()
                .setThrottleTimeMs(17).setClusterId("cluster-1").setControllerId(2)
                .setClusterAuthorizedOperations(123);
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(1).setHost("broker-a").setPort(9092).setRack("rack-a"));
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(2).setHost("broker-b").setPort(9093).setRack("rack-b"));
        MetadataResponseData.MetadataResponsePartition partition =
                new MetadataResponseData.MetadataResponsePartition().setPartitionIndex(4).setLeaderId(2)
                        .setLeaderEpoch(8).setReplicaNodes(List.of(1, 2)).setIsrNodes(List.of(2))
                        .setOfflineReplicas(List.of(1));
        response.topics().add(new MetadataResponseData.MetadataResponseTopic().setName("orders")
                .setIsInternal(false).setTopicAuthorizedOperations(99).setPartitions(List.of(partition)));
        return response;
    }
}

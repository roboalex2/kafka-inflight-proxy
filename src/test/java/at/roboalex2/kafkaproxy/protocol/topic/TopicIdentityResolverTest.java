package at.roboalex2.kafkaproxy.protocol.topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.api.error.BackendException;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.junit.jupiter.api.Test;

class TopicIdentityResolverTest {
    @Test
    void learnsBothDirectionsAndResolvesNameOnlyProtocolVersions() {
        TopicIdentityResolver resolver = new TopicIdentityResolver();
        UUID topicId = UUID.randomUUID();
        Uuid kafkaId = new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
        MetadataResponseData metadata = new MetadataResponseData();
        metadata.topics().add(new MetadataResponseData.MetadataResponseTopic()
                .setName("orders").setTopicId(kafkaId));
        resolver.observe(metadata);

        assertThat(resolver.resolveRequired("orders", Uuid.ZERO_UUID)).isEqualTo(topicId);
        assertThat(resolver.resolveName(topicId)).contains("orders");
    }

    @Test
    void directProtocolIdentityAlsoUpdatesBothDirections() {
        TopicIdentityResolver resolver = new TopicIdentityResolver();
        UUID topicId = UUID.randomUUID();
        Uuid kafkaId = new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());

        assertThat(resolver.resolveRequired("orders", kafkaId)).isEqualTo(topicId);
        assertThat(resolver.resolveName(topicId)).contains("orders");
        assertThatThrownBy(() -> resolver.resolveRequired("unknown", Uuid.ZERO_UUID))
                .isInstanceOf(BackendException.class);
    }
}

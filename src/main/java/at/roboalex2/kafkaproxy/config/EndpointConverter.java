package at.roboalex2.kafkaproxy.config;

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

/** Converts endpoint strings, including map keys, while configuration properties bind. */
@Component
@ConfigurationPropertiesBinding
public class EndpointConverter implements Converter<String, Endpoint> {
    @Override
    public Endpoint convert(String source) { return Endpoint.parse(source); }
}

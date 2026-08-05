package at.roboalex2.kafkaproxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaProxyApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProxyApplication.class, args);
    }
}

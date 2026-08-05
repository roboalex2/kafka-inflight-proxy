package at.roboalex2.kafkaproxy.network;

import org.springframework.context.SmartLifecycle;

/** Owns the Netty listener and all connections accepted through it. */
public interface KafkaProxyServer extends SmartLifecycle {
}

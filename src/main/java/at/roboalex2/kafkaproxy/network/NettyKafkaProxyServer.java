package at.roboalex2.kafkaproxy.network;

import at.roboalex2.kafkaproxy.config.Endpoint;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "kafka-proxy.server", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class NettyKafkaProxyServer implements KafkaProxyServer {
    private final Endpoint listenEndpoint;
    private final ClientChannelInitializer clientChannelInitializer;
    private final ConnectionRegistry connectionRegistry;

    private volatile boolean running;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyKafkaProxyServer(KafkaProxyProperties properties,
                                 ClientChannelInitializer clientChannelInitializer,
                                 ConnectionRegistry connectionRegistry) {
        this.listenEndpoint = properties.getListenAddress();
        this.clientChannelInitializer = clientChannelInitializer;
        this.connectionRegistry = connectionRegistry;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        connectionRegistry.startAccepting();
        try {
            bossGroup = new MultiThreadIoEventLoopGroup(1,
                    new DefaultThreadFactory("kafka-proxy-boss"), NioIoHandler.newFactory());
            workerGroup = new MultiThreadIoEventLoopGroup(0,
                    new DefaultThreadFactory("kafka-proxy-worker"), NioIoHandler.newFactory());
            ChannelFuture bindFuture = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.AUTO_READ, false)
                    .childHandler(clientChannelInitializer)
                    .bind(listenEndpoint.getHost(), listenEndpoint.getPort())
                    .awaitUninterruptibly();
            if (!bindFuture.isSuccess()) {
                throw new IllegalStateException("Failed to bind Kafka proxy listener at " + listenEndpoint,
                        bindFuture.cause());
            }
            serverChannel = bindFuture.channel();
            running = true;
        } catch (RuntimeException exception) {
            connectionRegistry.closeAll();
            shutdownEventLoops();
            throw exception;
        }
    }

    @Override
    public synchronized void stop() {
        if (!running && bossGroup == null && workerGroup == null) {
            return;
        }
        running = false;
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        connectionRegistry.closeAll();
        shutdownEventLoops();
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void shutdownEventLoops() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            bossGroup = null;
        }
    }
}

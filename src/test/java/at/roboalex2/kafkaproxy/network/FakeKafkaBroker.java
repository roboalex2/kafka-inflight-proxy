package at.roboalex2.kafkaproxy.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class FakeKafkaBroker implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService acceptExecutor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Socket> acceptedSockets = new LinkedBlockingQueue<>();
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();

    FakeKafkaBroker() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        acceptExecutor.submit(this::acceptConnections);
    }

    int getPort() {
        return serverSocket.getLocalPort();
    }

    Socket awaitConnection(Duration timeout) throws InterruptedException {
        Socket socket = acceptedSockets.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (socket == null) {
            throw new AssertionError("Proxy did not connect to the fake broker within " + timeout);
        }
        return socket;
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
        for (Socket socket : sockets) {
            socket.close();
        }
        acceptExecutor.shutdownNow();
    }

    private void acceptConnections() {
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(3_000);
                sockets.add(socket);
                acceptedSockets.add(socket);
            } catch (IOException exception) {
                if (!serverSocket.isClosed()) {
                    throw new IllegalStateException("Fake broker failed while accepting a connection", exception);
                }
            }
        }
    }
}

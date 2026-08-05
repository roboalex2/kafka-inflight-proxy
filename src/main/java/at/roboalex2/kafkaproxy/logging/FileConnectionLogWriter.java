package at.roboalex2.kafkaproxy.logging;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Called only by a connection's ordered inspection worker, never by a Netty event loop. */
public class FileConnectionLogWriter implements ConnectionLogWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileConnectionLogWriter.class);
    private final Path connectionDirectory;
    private BufferedWriter writer;
    private boolean failed;

    public FileConnectionLogWriter(Path connectionDirectory) {
        this.connectionDirectory = connectionDirectory;
    }

    @Override public boolean isEnabled() { return true; }

    @Override
    public void append(String entry) {
        if (failed) {
            return;
        }
        try {
            ensureOpen();
            writer.write(entry);
            writer.newLine();
            writer.flush();
        } catch (IOException exception) {
            failed = true;
            LOGGER.warn("Disabling protocol log for {} after an I/O failure", connectionDirectory, exception);
            closeQuietly();
        }
    }

    @Override
    public void close() {
        if (!failed) {
            try {
                ensureOpen();
            } catch (IOException exception) {
                failed = true;
                LOGGER.warn("Could not create protocol log for {}", connectionDirectory, exception);
            }
        }
        closeQuietly();
    }

    private void ensureOpen() throws IOException {
        if (writer == null) {
            Files.createDirectories(connectionDirectory);
            writer = Files.newBufferedWriter(connectionDirectory.resolve("connection.log"), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private void closeQuietly() {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException exception) {
            LOGGER.debug("Could not close protocol log {}", connectionDirectory, exception);
        } finally {
            writer = null;
        }
    }
}

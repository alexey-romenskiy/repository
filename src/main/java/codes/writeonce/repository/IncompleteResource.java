package codes.writeonce.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.SECONDS;

public class IncompleteResource implements Resource {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final CompletableFuture<Path> future = new CompletableFuture<>();

    @Nonnull
    private final Lock lock;

    @Nullable
    private final Authenticator authenticator;

    @Nonnull
    private final Path path;

    @Nonnull
    private final Path tmpPath;

    @Nonnull
    private final Condition condition;

    @Nonnull
    private final URI uri;

    private final OutputStream out;

    private boolean completed;

    private long receivedLength;

    private IOException exception;

    public IncompleteResource(@Nonnull Lock lock, @Nonnull ExecutorService threadPool,
            @Nullable Authenticator authenticator, @Nonnull Path path, @Nonnull URI uri) {
        this.lock = lock;
        this.authenticator = authenticator;
        this.path = path;
        this.tmpPath = getTmpPath(path);
        this.condition = lock.newCondition();
        this.uri = uri;
        this.out = open();
        if (out != null) {
            threadPool.execute(this::download);
        }
    }

    @Nonnull
    private static Path getTmpPath(@Nonnull Path path) {
        return path.getParent().resolve(path.getFileName().toString() + ".tmp");
    }

    private OutputStream open() {
        try {
            Files.createDirectories(tmpPath.getParent());
            return Files.newOutputStream(tmpPath);
        } catch (Throwable e) {
            close(new IOException("Downloading failed: " + uri, e));
            if (e instanceof final Error error) {
                throw error;
            }
            return null;
        }
    }

    @Nonnull
    @Override
    public InputStream getInputStream() throws IOException {
        lock.lock();
        try {
            if (exception != null) {
                throw exception;
            }
            if (completed) {
                return Files.newInputStream(path);
            }
            return new InputStreamImpl(Files.newInputStream(tmpPath));
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Path> getCompletableFuture() {
        return future;
    }

    @Nonnull
    @Override
    public Path getPath() {
        return path;
    }

    private void download() {

        logger.info("Downloading started: {}", uri);

        final var client = getClient();
        final var request = HttpRequest.newBuilder(uri).timeout(Duration.of(5, HOURS)).build();
        final var start = System.nanoTime();
        try {
            final var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (var in = response.body()) {
                final var statusCode = response.statusCode();
                if (statusCode == 200) {
                    final var bytes = new byte[0x1000000];
                    while (true) {
                        final var read = in.read(bytes);
                        if (read == -1) {
                            break;
                        }
                        if (read > 0) {
                            out.write(bytes, 0, read);
                            lock.lock();
                            try {
                                receivedLength += read;
                                condition.signalAll();
                            } finally {
                                lock.unlock();
                            }
                        }
                    }
                    complete();
                    logger.info("Downloading completed in {} nanos: {}", System.nanoTime() - start, uri);
                } else {
                    throw new IOException("HTTP status code: " + statusCode);
                }
            }
        } catch (Throwable e) {
            logger.error("Downloading failed: {}", uri, e);
            close(new IOException("Downloading failed: " + uri, e));
            if (e instanceof final Error error) {
                throw error;
            }
        }
    }

    @Nonnull
    private HttpClient getClient() {
        final var clientBuilder = HttpClient.newBuilder().connectTimeout(Duration.of(5, SECONDS));
        if (authenticator != null) {
            clientBuilder.authenticator(authenticator);
        }
        return clientBuilder.build();
    }

    private void complete() throws IOException {
        lock.lock();
        try {
            if (!completed) {
                out.close();
                Files.move(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
                condition.signalAll();
                completed = true;
                future.complete(path);
            }
        } finally {
            lock.unlock();
        }
    }

    private void close(@Nonnull IOException exception) {
        lock.lock();
        try {
            if (!completed) {
                this.exception = exception;
                completed = true;
                future.completeExceptionally(exception);
                condition.signalAll();
                if (out != null) {
                    out.close();
                }
                Files.deleteIfExists(tmpPath);
            }
        } catch (Throwable e) {
            exception.addSuppressed(e);
            logger.error("Failed to delete temporary file {}", tmpPath, e);
            if (e instanceof final Error error) {
                throw error;
            }
        } finally {
            lock.unlock();
        }
    }

    private class InputStreamImpl extends InputStream {

        @Nonnull
        private final InputStream inputStream;

        private long readLength;

        private boolean closed;

        public InputStreamImpl(@Nonnull InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            lock.lock();
            try {
                if (exception != null) {
                    throw exception;
                }
                while (receivedLength == readLength) {
                    condition.awaitUninterruptibly();
                    if (exception != null) {
                        throw exception;
                    }
                }
            } finally {
                lock.unlock();
            }
            final var value = inputStream.read();
            readLength++;
            return value;
        }

        @Override
        public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            if (length <= 0) {
                return 0;
            }
            long available;
            lock.lock();
            try {
                if (exception != null) {
                    throw exception;
                }
                while (true) {
                    available = receivedLength - readLength;
                    if (completed && available == 0) {
                        return -1;
                    }
                    if (available != 0) {
                        break;
                    }
                    condition.awaitUninterruptibly();
                    if (exception != null) {
                        throw exception;
                    }
                }
            } finally {
                lock.unlock();
            }
            final var read = inputStream.read(buffer, offset, (int) Math.min(length, available));
            if (read < 1) {
                throw new IOException("Premature end of file: " + tmpPath);
            }
            readLength += read;
            return read;
        }

        @Override
        public long skip(long length) throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            if (length <= 0) {
                return 0;
            }
            long available;
            lock.lock();
            try {
                if (exception != null) {
                    throw exception;
                }
                while (true) {
                    available = receivedLength - readLength;
                    if (completed && available == 0) {
                        return 0;
                    }
                    if (available != 0) {
                        break;
                    }
                    condition.awaitUninterruptibly();
                    if (exception != null) {
                        throw exception;
                    }
                }
            } finally {
                lock.unlock();
            }
            final var skipped = inputStream.skip(Math.min(length, available));
            readLength += skipped;
            return skipped;
        }

        @Override
        public void skipNBytes(long length) throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            while (length > 0) {
                long available;
                lock.lock();
                try {
                    if (exception != null) {
                        throw exception;
                    }
                    while (true) {
                        available = receivedLength - readLength;
                        if (completed && available == 0) {
                            throw new EOFException();
                        }
                        if (available != 0) {
                            break;
                        }
                        condition.awaitUninterruptibly();
                        if (exception != null) {
                            throw exception;
                        }
                    }
                } finally {
                    lock.unlock();
                }
                final var skipped = Math.min(length, available);
                inputStream.skipNBytes(skipped);
                readLength += skipped;
                length -= skipped;
            }
        }

        @Override
        public int available() {
            if (closed) {
                return 0;
            }
            lock.lock();
            try {
                return (int) Math.min(receivedLength - readLength, Integer.MAX_VALUE);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            inputStream.close();
        }
    }
}

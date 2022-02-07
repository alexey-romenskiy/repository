package codes.writeonce.repository;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class CompleteResource implements Resource {

    @Nonnull
    private final Path path;

    @Nonnull
    private final CompletableFuture<Path> future;

    public CompleteResource(@Nonnull Path path) {
        this.path = path;
        this.future = CompletableFuture.completedFuture(path);
    }

    @Nonnull
    @Override
    public InputStream getInputStream() throws IOException {
        return Files.newInputStream(path);
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
}

package codes.writeonce.repository;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface Resource {

    @Nonnull
    InputStream getInputStream() throws IOException;

    @Nonnull
    CompletableFuture<Path> getCompletableFuture();

    @Nonnull
    Path getPath();
}

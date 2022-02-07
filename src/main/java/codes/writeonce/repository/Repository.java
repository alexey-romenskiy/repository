package codes.writeonce.repository;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

public class Repository implements AutoCloseable {

    @Nonnull
    private final String baseUri;

    @Nonnull
    public final Path launcherDir;

    @Nonnull
    public final Path repoPath;

    @Nonnull
    private final ExecutorService threadPool = Executors.newFixedThreadPool(4);

    @Nullable
    private final Authenticator authenticator;

    private final Lock lock = new ReentrantLock();

    private final Map<Path, Resource> resources = new HashMap<>();

    public Repository() throws IOException {

        final var home = Path.of(System.getProperty("user.home")).toRealPath();
        launcherDir = home.resolve(".launcher");
        final var configPath = launcherDir.resolve("settings.properties");
        final var configProperties = getConfigProperties(configPath);
        baseUri = requireNonNull(configProperties.getProperty("baseUri"));
        repoPath = launcherDir.resolve("repository");
        Files.createDirectories(repoPath);

        final var username = configProperties.getProperty("username");
        final var password = configProperties.getProperty("password");
        if (username == null) {
            if (password != null) {
                throw new IllegalArgumentException();
            }
            authenticator = null;
        } else {
            if (password == null) {
                throw new IllegalArgumentException();
            }
            authenticator = getAuthenticator(username, password);
        }
    }

    @Nonnull
    private static Properties getConfigProperties(@Nonnull Path configPath) throws IOException {
        if (!Files.isRegularFile(configPath)) {
            throw new IllegalArgumentException();
        }
        final var configProperties = new Properties();
        try (var in = Files.newInputStream(configPath)) {
            configProperties.load(in);
        }
        return configProperties;
    }

    @Override
    public void close() {
        threadPool.shutdown();
    }

    @Nonnull
    public Resource resolve(@Nonnull String value) throws IOException {
        return resolve(parseArtifact(value));
    }

    @Nonnull
    public Resource resolve(@Nonnull Artifact artifact) throws IOException {

        if (artifact.isSnapshot()) {
            final var resolvedArtifact = findVersion(artifact);
            return fetch(getArtifactUri(resolvedArtifact), getArtifactPath(resolvedArtifact), false);
        } else {
            return fetch(getArtifactUri(artifact), getArtifactPath(artifact), false);
        }
    }

    @Nonnull
    private Artifact findVersion(@Nonnull Artifact artifact) throws IOException {

        final var dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder db;
        try {
            db = dbf.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
        final Document d;
        try {
            d = db.parse(fetch(getMetadataUri(artifact), getMetadataPath(artifact), true).getInputStream());
        } catch (SAXException e) {
            throw new IOException(e);
        }
        final var metadata = d.getDocumentElement();
        final var groupId = metadata.getElementsByTagName("groupId").item(0).getTextContent();
        final var artifactId = metadata.getElementsByTagName("artifactId").item(0).getTextContent();
        final var baseVersion = metadata.getElementsByTagName("version").item(0).getTextContent();
        if (!groupId.equals(artifact.groupId) || !artifactId.equals(artifact.artifactId) || !baseVersion.equals(
                artifact.getBaseVersion())) {
            throw new IllegalArgumentException();
        }
        final var versioning = (Element) metadata.getElementsByTagName("versioning").item(0);
        final var snapshotVersions = (Element) versioning.getElementsByTagName("snapshotVersions").item(0);
        final var snapshotVersion = snapshotVersions.getElementsByTagName("snapshotVersion");
        for (int i = 0; i < snapshotVersion.getLength(); i++) {
            final var item = (Element) snapshotVersion.item(i);
            final var c = item.getElementsByTagName("classifier");
            final var classifier = c.getLength() == 0 ? null : c.item(0).getTextContent();
            final var extension = item.getElementsByTagName("extension").item(0).getTextContent();
            final var version = item.getElementsByTagName("value").item(0).getTextContent();
            if (Objects.equals(classifier, artifact.classifier) && extension.equals(artifact.type)) {
                return new Artifact(artifact.groupId, artifact.artifactId, artifact.type, artifact.classifier, version);
            }
        }
        throw new RuntimeException();
    }

    @Nonnull
    private Resource fetch(@Nonnull URI uri, @Nonnull Path path, boolean force) {

        lock.lock();
        try {
            final var existing = resources.get(path);
            if (existing != null) {
                return existing;
            }
            if (!force && Files.exists(path)) {
                return new CompleteResource(path);
            }
            final var resource = new IncompleteResource(lock, threadPool, authenticator, path, uri);
            resources.put(path, resource);
            return resource;
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    private static Authenticator getAuthenticator(@Nonnull String username, @Nonnull String password) {
        return new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password.toCharArray());
            }
        };
    }

    @Nonnull
    private static Artifact parseArtifact(@Nonnull String value) {
        final var strings = value.split(":", -1);
        return switch (strings.length) {
            case 4 -> new Artifact(strings[0], strings[1], strings[2], null, strings[3]);
            case 5 -> new Artifact(strings[0], strings[1], strings[2], strings[3], strings[4]);
            default -> throw new IllegalArgumentException();
        };
    }

    @Nonnull
    private URI getMetadataUri(@Nonnull Artifact artifact) {
        return getUri(artifact, "maven-metadata.xml");
    }

    @Nonnull
    private URI getArtifactUri(@Nonnull Artifact artifact) {
        return getUri(artifact, getFileName(artifact));
    }

    @Nonnull
    private URI getUri(@Nonnull Artifact artifact, String fileName) {

        var index = baseUri.length();
        while (index > 0 && baseUri.charAt(index - 1) == '/') {
            index--;
        }

        final var builder = new StringBuilder();
        builder.append(baseUri, 0, index);

        for (final var name : artifact.groupId.split("\\.", -1)) {
            builder.append('/').append(name);
        }

        builder.append('/').append(artifact.artifactId).append('/').append(artifact.getBaseVersion()).append('/')
                .append(fileName);

        return URI.create(builder.toString());
    }

    @Nonnull
    private Path getArtifactPath(@Nonnull Artifact artifact) {

        var filePath = repoPath;

        for (final var name : artifact.groupId.split("\\.", -1)) {
            filePath = filePath.resolve(name);
        }

        final String fileName = getFileName(artifact);

        return filePath.resolve(artifact.artifactId).resolve(artifact.getBaseVersion()).resolve(fileName);
    }

    @Nonnull
    private Path getMetadataPath(@Nonnull Artifact artifact) {

        var filePath = repoPath;

        for (final var name : artifact.groupId.split("\\.", -1)) {
            filePath = filePath.resolve(name);
        }

        return filePath.resolve(artifact.artifactId).resolve(artifact.getBaseVersion()).resolve("maven-metadata.xml");
    }

    @Nonnull
    private static String getFileName(@Nonnull Artifact artifact) {

        final var fileNameBuilder = new StringBuilder();
        fileNameBuilder.append(artifact.artifactId).append('-').append(artifact.version);
        if (artifact.classifier != null) {
            fileNameBuilder.append('-').append(artifact.classifier);
        }
        fileNameBuilder.append('.').append(artifact.type);
        return fileNameBuilder.toString();
    }
}

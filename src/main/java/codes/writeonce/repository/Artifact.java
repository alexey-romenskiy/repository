package codes.writeonce.repository;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class Artifact {

    private static final Pattern VERSION_FILE_PATTERN = Pattern.compile("^(.*)-([0-9]{8}.[0-9]{6})-([0-9]+)$");

    private static final Pattern PATTERN = Pattern.compile("^\\.{1,2}$|[:;/\\\\]");

    private static final String SNAPSHOT = "-SNAPSHOT";

    @Nonnull
    public final String groupId;

    @Nonnull
    public final String artifactId;

    @Nonnull
    public final String type;

    @Nullable
    public final String classifier;

    @Nonnull
    public final String version;

    public Artifact(@Nonnull String groupId, @Nonnull String artifactId, @Nonnull String type,
            @Nullable String classifier, @Nonnull String version) {

        if (groupId.isEmpty() || PATTERN.matcher(groupId).find() || groupId.startsWith(".") || groupId.endsWith(".") ||
            groupId.contains("..")) {
            throw new IllegalArgumentException();
        }

        if (artifactId.isEmpty() || PATTERN.matcher(artifactId).find()) {
            throw new IllegalArgumentException();
        }

        if (type.isEmpty() || PATTERN.matcher(type).find()) {
            throw new IllegalArgumentException();
        }

        if (classifier != null && (classifier.isEmpty() || PATTERN.matcher(classifier).find())) {
            throw new IllegalArgumentException();
        }

        if (version.isEmpty() || PATTERN.matcher(version).find()) {
            throw new IllegalArgumentException();
        }

        this.groupId = groupId;
        this.artifactId = artifactId;
        this.type = type;
        this.classifier = classifier;
        this.version = version;
    }

    public boolean isSnapshot() {
        return version.endsWith(SNAPSHOT);
    }

    @Nonnull
    public String getBaseVersion() {
        final var matcher = VERSION_FILE_PATTERN.matcher(version);
        if (matcher.find()) {
            return matcher.group(1) + SNAPSHOT;
        }
        return version;
    }
}

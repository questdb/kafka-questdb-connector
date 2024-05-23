package io.questdb.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class VersionUtil {
    private static final String VERSION;
    private static final String GIT_HASH;
    private static final String UNKNOWN = "unknown";
    private static final String PROPERTIES_FILE = "questdb_connector_version.properties"; // keep in sync with pom.xml

    static {
        Properties properties = new Properties();
        String version;
        String gitHash;
        try (InputStream is = VersionUtil.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)){
            if (is == null) {
                version = UNKNOWN;
                gitHash = UNKNOWN;
            } else {
                properties.load(is);
                version = String.valueOf(properties.getOrDefault("git.build.version", UNKNOWN));
                gitHash = String.valueOf(properties.getOrDefault("git.commit.id.abbrev", UNKNOWN));
            }
        } catch (IOException e) {
            version = UNKNOWN;
            gitHash = UNKNOWN;
        }
        VERSION = version;
        GIT_HASH = gitHash;
    }

    static String getVersion() {
        return VERSION;
    }

    static String getGitHash() {
        return GIT_HASH;
    }
}

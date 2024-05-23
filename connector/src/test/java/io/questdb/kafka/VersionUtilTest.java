package io.questdb.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VersionUtilTest {
    // if these tests are failing then build the project with maven
    // maven build generates the questdb_connector_version.properties file

    @Test
    public void testGetVersion() {
        String version = VersionUtil.getVersion();
        assertNotNull(version);
        assertNotEquals("unknown", version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void testGetGitHash() {
        String gitHash = VersionUtil.getGitHash();
        assertNotNull(gitHash);
        assertNotEquals("unknown", gitHash);
        assertFalse(gitHash.isEmpty());
    }
}
package io.questdb.kafka;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the two server behaviours the QWP task's delivery model relies on. These
 * are properties of QuestDB, not of the connector, so they are asserted against
 * a real server rather than argued from source.
 */
@Testcontainers
class QwpServerSemanticsTest {
    private static final long TS = 1_700_000_000_000_000L; // fixed: a dedup key must repeat across replays
    private static final long VISIBILITY_MARGIN_MS = 1_500; // publish->queryable is ~1ms in practice

    @Container
    private static final GenericContainer<?> QUESTDB = new GenericContainer<>(DockerImageName.parse("questdb/questdb:10.0.0"))
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT);

    private int httpPort;
    private String confString;

    @BeforeEach
    void setUp() {
        httpPort = QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        confString = "ws::addr=" + QUESTDB.getHost() + ":" + httpPort + ";";
        QuestDBUtils.dropAllTables(httpPort);
    }

    /**
     * A rejected multi-table batch must not leave a partially applied prefix
     * behind: replay isolation resends the whole flush entry, so a partial
     * commit would silently duplicate the tables that did commit.
     */
    @Test
    void rejectedMultiTableBatchAppliesNothing() throws Exception {
        String good = createTable("atomic_good", false);
        String other = createTable("atomic_other", false);
        String strict = createTable("atomic_strict", false);

        try (Sender sender = Sender.fromConfig(confString)) {
            sender.table(good).symbol("id", "a").longColumn("v", 1).at(TS, ChronoUnit.MICROS);
            sender.table(other).symbol("id", "a").longColumn("v", 1).at(TS, ChronoUnit.MICROS);
            // v is LONG server-side; the client cannot know that locally, so the
            // server rejects the batch with SCHEMA_MISMATCH
            sender.table(strict).symbol("id", "a").stringColumn("v", "not-a-long").at(TS, ChronoUnit.MICROS);
            sender.flush();

            LineSenderServerException rejection = assertThrows(LineSenderServerException.class, () -> sender.drain(10_000));
            assertTrue(String.valueOf(rejection.getMessage()).contains("SCHEMA_MISMATCH"),
                    "expected a schema mismatch, got: " + rejection.getMessage());
        } catch (LineSenderServerException expectedOnClose) {
            // close() re-surfaces the latched terminal; the assertions above already ran
        }

        Thread.sleep(VISIBILITY_MARGIN_MS);
        for (String table : new String[]{good, other, strict}) {
            QuestDBUtils.assertSql("\"count()\"\r\n0\r\n", "select count() from " + table, httpPort);
        }
    }

    /**
     * Replaying an unacknowledged batch duplicates rows - the reason the
     * connector documents at-least-once and recommends DEDUP UPSERT KEYS.
     */
    @Test
    void replayDuplicatesUnlessTableDedups() throws Exception {
        String plain = createTable("replay_plain", false);
        String deduped = createTable("replay_dedup", true);

        for (int attempt = 0; attempt < 2; attempt++) {
            try (Sender sender = Sender.fromConfig(confString)) {
                sender.table(plain).symbol("id", "a").longColumn("v", 1).at(TS, ChronoUnit.MICROS);
                sender.table(deduped).symbol("id", "a").longColumn("v", 1).at(TS, ChronoUnit.MICROS);
                sender.flush();
                sender.drain(10_000);
            }
        }

        QuestDBUtils.assertSqlEventually("\"count()\"\r\n2\r\n", "select count() from " + plain, httpPort);
        QuestDBUtils.assertSqlEventually("\"count()\"\r\n1\r\n", "select count() from " + deduped, httpPort);
    }

    private String createTable(String name, boolean dedup) {
        String ddl = "CREATE TABLE " + name + " (ts TIMESTAMP, id SYMBOL, v LONG) timestamp(ts) PARTITION BY DAY WAL"
                + (dedup ? " DEDUP UPSERT KEYS(ts, id)" : "");
        QuestDBUtils.assertSql("{\"ddl\":\"OK\"}", ddl, httpPort, QuestDBUtils.Endpoint.EXEC);
        return name;
    }
}

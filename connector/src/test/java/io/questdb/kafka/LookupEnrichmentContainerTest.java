package io.questdb.kafka;

import io.questdb.client.QueryException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lookup enrichment against a real server: what {@link LookupEnrichment#load()} accepts, rejects
 * and recovers from, and how a real Sender takes the enriched column.
 */
@Testcontainers(disabledWithoutDocker = true)
class LookupEnrichmentContainerTest {
    @Container
    private static final GenericContainer<?> QUESTDB = new GenericContainer<>(DockerImageName.parse("questdb/questdb:10.0.0"))
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            // well above what the other tests' queries take; lets one test hit the server's limit
            .withEnv("QDB_QUERY_TIMEOUT", "2000ms");

    /** Both sides render the key as text: an integer key is plain decimal, as the record side writes it. */
    @ParameterizedTest
    @CsvSource({
            "LONG, SYMBOL", "INT, SYMBOL", "SHORT, SYMBOL", "BYTE, SYMBOL",
            "VARCHAR, SYMBOL", "STRING, SYMBOL", "SYMBOL, SYMBOL",
            "LONG, VARCHAR", "LONG, STRING"
    })
    void loadsEveryAcceptedKeyAndValueType(String keyType, String valueType) {
        String table = createTable("k " + keyType + ", v " + valueType);
        boolean textKey = keyType.equals("VARCHAR") || keyType.equals("STRING") || keyType.equals("SYMBOL");
        insert(table, "cast(" + (textKey ? "'-5'" : "-5") + " as " + keyType + "), cast('neg' as " + valueType + ')');
        insert(table, "cast(" + (textKey ? "'42'" : "42") + " as " + keyType + "), cast('pos' as " + valueType + ')');

        LookupEnrichment.Lookup lookup = load("select k, v from " + table);

        assertEquals("v", lookup.column);
        Map<String, String> expected = new HashMap<>();
        expected.put("-5", "neg");
        expected.put("42", "pos");
        assertEquals(expected, lookup.map);
    }

    @Test
    void skipsNullsAndKeepsTheLastRowForADuplicateKey() {
        String table = createTable("k LONG, v SYMBOL");
        insert(table, "1, 'first'");
        insert(table, "1, 'last'");
        insert(table, "null, 'no key'");
        insert(table, "2, null");

        assertEquals(Map.of("1", "last"), load("select k, v from " + table).map);
    }

    @Test
    void anEmptyResultStillNamesTheValueColumn() {
        String table = createTable("k LONG, v SYMBOL");

        LookupEnrichment.Lookup lookup = load("select k, v from " + table);

        assertEquals("v", lookup.column);
        assertTrue(lookup.map.isEmpty());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select k, v, ts from %s | must return exactly two columns",
            "select k, 1.5 v from %s | value column 'v' must be VARCHAR, STRING or SYMBOL",
            "select ts, v from %s | key column 'ts' must be VARCHAR, STRING, SYMBOL or an integer type",
            // runs once, then the load fails rather than running it again on every refresh
            "create table if not exists %s_created (a int) | must be a query"
    })
    void rejectsAResultItCannotUse(String sqlTemplate, String expectedMessage) {
        String table = createTable("k LONG, v SYMBOL, ts TIMESTAMP");
        insert(table, "1, 'a', now()");

        ConnectException failure = assertThrows(ConnectException.class, () -> load(String.format(sqlTemplate, table)));

        assertTrue(failure.getMessage().contains(expectedMessage), failure.getMessage());
    }

    /** The task fails on this at the first load rather than retrying: it is not transient. */
    @Test
    void aMissingTableIsAServerRejection() {
        QueryException failure = assertThrows(QueryException.class, () -> load("select k, v from does_not_exist"));

        assertFalse(LookupEnrichment.isTransient(failure), failure.getStatus() + ": " + failure.getMessage());
    }

    /**
     * The server's own query timeout, below enrichment.query.timeout.ms here, arrives as a limit
     * rather than as a rejection of the SQL: the first load retries it.
     */
    @Test
    void aServerQueryTimeoutIsTransient() {
        // a valid (key, value) shape whose filter passes nothing, so it runs until it is stopped
        QueryException failure = assertThrows(QueryException.class,
                () -> load("select x, rnd_str(5,5,0) v from long_sequence(100000000000) where rnd_double() > 2"));

        assertEquals(QwpConstants.STATUS_LIMIT_EXCEEDED, failure.getStatus(), failure.getMessage());
        assertTrue(LookupEnrichment.isTransient(failure), failure.getMessage());
    }

    /**
     * With failover=off (or failover_max_attempts=1) the query client keeps reporting a lost
     * connection on every later query. The handle must not outlive the failure.
     */
    @Test
    void recoversAfterTheConnectionIsLost() throws IOException {
        String table = createTable("k LONG, v SYMBOL");
        insert(table, "1, 'a'");
        try (TcpProxy proxy = new TcpProxy(QUESTDB.getHost(), QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT))) {
            LookupEnrichment enrichment = new LookupEnrichment(
                    "ws::addr=127.0.0.1:" + proxy.port() + ";failover=off;", "select k, v from " + table, 5_000L);
            try {
                assertEquals(Map.of("1", "a"), enrichment.load().map);

                proxy.severAll();
                QueryException lost = assertThrows(QueryException.class, enrichment::load);
                assertTrue(LookupEnrichment.isTransient(lost), lost.getStatus() + ": " + lost.getMessage());

                assertEquals(Map.of("1", "a"), enrichment.load().map);
            } finally {
                enrichment.close();
            }
        }
    }

    /**
     * The client fixes a column's type for a table once a row has written it, so a record field
     * named like the enriched column defeats the enrichment of its own row and of every later
     * row of the table. The failure must name the column; records without a mapping still land.
     */
    @Test
    void aRecordFieldNamedLikeTheEnrichedColumnFailsNamingTheColumn() {
        String table = ConnectTestUtils.newTableName();
        Map<String, String> props = new HashMap<>();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, confString());
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, table);
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.ENRICHMENT_SQL_CONFIG, "select k, v from unused");
        props.put(QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, "sec_id");
        try (Sender sender = Sender.builder(confString()).build()) {
            RecordToRowHandler handler = new RecordToRowHandler(new QuestDBSinkConnectorConfig(props), sender, true,
                    RecordToRowHandler.SenderErrorPolicy.PROBE, true, RecordToRowHandler.NameLimits.QWP);
            handler.setEnrichment("ticker", Map.of("42", "ACME"));

            // the record's own 'ticker' is a VARCHAR in the same row
            InvalidDataException sameRow = assertThrows(InvalidDataException.class,
                    () -> handler.handle(record(table, 42, "own")));
            assertTrue(sameRow.getMessage().contains("enrichment column 'ticker'"), sameRow.getMessage());

            // no mapping: written, and from now on 'ticker' is a VARCHAR of this table
            handler.handle(record(table, 7, "own"));
            InvalidDataException laterRow = assertThrows(InvalidDataException.class,
                    () -> handler.handle(record(table, 42, null)));
            assertTrue(laterRow.getMessage().contains("enrichment column 'ticker'"), laterRow.getMessage());
            sender.flush();
        }
        QuestDBUtils.assertSqlEventually("\"sec_id\",\"ticker\"\r\n7,\"own\"\r\n",
                "select sec_id, ticker from " + table, httpPort());
    }

    private static SinkRecord record(String table, long secId, String ticker) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("sec_id", secId);
        if (ticker != null) {
            value.put("ticker", ticker);
        }
        return new SinkRecord(table, 0, null, null, null, value, 0L);
    }

    private static String confString() {
        return "ws::addr=" + QUESTDB.getHost() + ':' + QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT) + ';';
    }

    private static LookupEnrichment.Lookup load(String sql) {
        LookupEnrichment enrichment = new LookupEnrichment(confString(), sql, 10_000L);
        try {
            return enrichment.load();
        } finally {
            enrichment.close();
        }
    }

    /** A non-WAL table, so inserted rows are visible to the next query and kept in insertion order. */
    private static String createTable(String columns) {
        String table = ConnectTestUtils.newTableName();
        QuestDBUtils.assertSql("{\"ddl\":\"OK\"}", "create table " + table + " (" + columns + ")",
                httpPort(), QuestDBUtils.Endpoint.EXEC);
        return table;
    }

    private static void insert(String table, String values) {
        QuestDBUtils.assertSql("{\"dml\":\"OK\"}", "insert into " + table + " values (" + values + ")",
                httpPort(), QuestDBUtils.Endpoint.EXEC);
    }

    private static int httpPort() {
        return QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
    }

    /** Forwards TCP connections to the server and can drop all of them at once. */
    private static final class TcpProxy implements Closeable {
        private final ServerSocket listener;
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();
        private final String upstreamHost;
        private final int upstreamPort;

        TcpProxy(String upstreamHost, int upstreamPort) throws IOException {
            this.upstreamHost = upstreamHost;
            this.upstreamPort = upstreamPort;
            this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(this::accept, "tcp-proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        void severAll() {
            for (Socket socket : sockets) {
                closeQuietly(socket);
            }
            sockets.clear();
        }

        @Override
        public void close() throws IOException {
            listener.close();
            severAll();
        }

        private void accept() {
            try {
                while (true) {
                    Socket client = listener.accept();
                    Socket upstream = new Socket(upstreamHost, upstreamPort);
                    sockets.add(client);
                    sockets.add(upstream);
                    pump(client, upstream);
                    pump(upstream, client);
                }
            } catch (IOException ignored) {
                // the listener closed
            }
        }

        private static void pump(Socket from, Socket to) {
            Thread pump = new Thread(() -> {
                byte[] buffer = new byte[64 * 1024];
                try {
                    InputStream in = from.getInputStream();
                    OutputStream out = to.getOutputStream();
                    for (int n; (n = in.read(buffer)) >= 0; ) {
                        out.write(buffer, 0, n);
                        out.flush();
                    }
                } catch (IOException ignored) {
                    // severed
                } finally {
                    closeQuietly(from);
                    closeQuietly(to);
                }
            }, "tcp-proxy-pump");
            pump.setDaemon(true);
            pump.start();
        }

        private static void closeQuietly(Socket socket) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
    }
}

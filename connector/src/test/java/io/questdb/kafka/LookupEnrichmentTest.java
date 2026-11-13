package io.questdb.kafka;

import io.questdb.client.QueryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class LookupEnrichmentTest {

    /**
     * The query handle is built from the connector's own conf string, sender-only keys
     * included, and must not connect anywhere until a query is borrowed. Nothing listens on
     * the address: had build() rejected a key or opened an ingest connection, load() would
     * fail with something other than the query client's own connect failure.
     */
    @Test
    void buildsFromConnectorConfStringAndConnectsOnlyToQuery() {
        LookupEnrichment enrichment = new LookupEnrichment(
                "ws::addr=localhost:1;auto_flush_rows=10;auto_flush_interval=1000;auto_flush_bytes=1000000;"
                        + "sf_max_total_bytes=67108864;sf_append_deadline_millis=30000;"
                        + "reconnect_max_duration_millis=1000;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=100;"
                        + "close_flush_timeout_millis=0;initial_connect_retry=off;",
                "select 1", 1_000L);
        try {
            QueryException failure = assertThrows(QueryException.class, enrichment::load);
            assertEquals(0, failure.getStatus());
            assertTrue(failure.getMessage().contains("failed to create query client"), failure.getMessage());
            assertTrue(LookupEnrichment.isTransient(failure));
        } finally {
            enrichment.close();
        }
    }

    /**
     * A host that drops packets. The client connects when the query is borrowed, before the
     * query timeout starts; without a connect timeout that connect waits for the OS to give up
     * (over two minutes on Linux), with put() blocked the whole time.
     * <p>
     * Simulated on loopback: once a listener's accept queue is full, Linux drops further SYNs,
     * so the next connect hangs as it would against a black-holed address.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void connectingToAHostThatDropsPacketsIsBoundedByTheQueryTimeout() throws IOException {
        try (ServerSocket neverAccepts = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Socket> queued = new ArrayList<>();
            try {
                boolean queueFull = false;
                for (int i = 0; i < 64 && !queueFull; i++) {
                    Socket socket = new Socket();
                    try {
                        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), neverAccepts.getLocalPort()), 200);
                        queued.add(socket);
                    } catch (SocketTimeoutException e) {
                        socket.close();
                        queueFull = true;
                    }
                }
                assumeTrue(queueFull, "the kernel did not start dropping SYNs");
                assertLoadFailsWithin("ws::addr=127.0.0.1:" + neverAccepts.getLocalPort() + ';');
            } finally {
                for (Socket socket : queued) {
                    socket.close();
                }
            }
        }
    }

    /** A peer that accepts the connection but never answers the WebSocket upgrade (15 s by default). */
    @Test
    void anUnansweredUpgradeIsBoundedByTheQueryTimeout() throws IOException {
        try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) {
                        accepted.add(silent.accept());
                    }
                } catch (IOException ignored) {
                    // the listener closed
                }
            }, "silent-peer");
            acceptor.setDaemon(true);
            acceptor.start();
            try {
                assertLoadFailsWithin("ws::addr=127.0.0.1:" + silent.getLocalPort() + ';');
            } finally {
                synchronized (accepted) {
                    for (Socket socket : accepted) {
                        socket.close();
                    }
                }
            }
        }
    }

    @Test
    void queryTimeoutsAreAddedOnlyWhereTheConfStringDoesNotSetThem() {
        assertEquals("ws::addr=h:9000;connect_timeout=1000;auth_timeout_ms=1000;",
                LookupEnrichment.withQueryTimeouts("ws::addr=h:9000", 1_000L));
        assertEquals("ws::addr=h:9000;connect_timeout=500;auth_timeout_ms=1000;",
                LookupEnrichment.withQueryTimeouts("ws::addr=h:9000;connect_timeout=500;", 1_000L));
        assertEquals("wss::auth_timeout_ms=700;addr=h:9000;connect_timeout=1000;",
                LookupEnrichment.withQueryTimeouts("wss::auth_timeout_ms=700;addr=h:9000;", 1_000L));
        // a trailing ';;' is an escaped semicolon inside the value, not a terminator
        assertEquals("ws::addr=h:9000;password=a;;;connect_timeout=1000;auth_timeout_ms=1000;",
                LookupEnrichment.withQueryTimeouts("ws::addr=h:9000;password=a;;", 1_000L));
        // connect_timeout is an int on the client side
        assertEquals("ws::addr=h:9000;connect_timeout=" + Integer.MAX_VALUE + ";auth_timeout_ms=" + Long.MAX_VALUE + ';',
                LookupEnrichment.withQueryTimeouts("ws::addr=h:9000;", Long.MAX_VALUE));
        // malformed: passed through for the client to report
        assertEquals("ws::addr", LookupEnrichment.withQueryTimeouts("ws::addr", 1_000L));
    }

    private static void assertLoadFailsWithin(String confString) {
        LookupEnrichment enrichment = new LookupEnrichment(confString, "select 1", 1_000L);
        try {
            QueryException failure = assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> assertThrows(QueryException.class, enrichment::load));
            assertTrue(LookupEnrichment.isTransient(failure), failure.getMessage());
        } finally {
            enrichment.close();
        }
    }
}

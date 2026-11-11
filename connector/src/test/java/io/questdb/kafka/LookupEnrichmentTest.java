package io.questdb.kafka;

import io.questdb.client.QueryException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}

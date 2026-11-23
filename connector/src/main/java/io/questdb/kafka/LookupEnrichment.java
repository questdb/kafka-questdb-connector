package io.questdb.kafka;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.str.StringSink;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Runs the user's lookup query over the QWP query API and returns its result as a map.
 * Called by one thread at a time: the task thread for the first load, then the task's
 * refresher. Holds no lookup state of its own.
 */
final class LookupEnrichment {
    private final String confString;
    private final long queryTimeoutMs;
    private final String sql;
    private QuestDB db;

    LookupEnrichment(String confString, String sql, long queryTimeoutMs) {
        this.confString = withQueryTimeouts(confString, queryTimeoutMs);
        this.sql = sql;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    /**
     * True for failures that say nothing about the query itself, which the first load retries.
     * The client reports its own failures (connect, pool, timeout) with status 0, and a
     * connection lost mid-query with STATUS_INTERNAL_ERROR - which a server-side internal error
     * shares, so that one is retried too. The server reports its query timeout, circuit breaker
     * and memory limit as STATUS_LIMIT_EXCEEDED, and a query cancelled there as STATUS_CANCELLED;
     * both can pass on a later attempt. The one limit that cannot, a single row larger than the
     * send buffer, is then retried until qwp.progress.timeout.ms. Everything else is the server
     * rejecting the SQL.
     */
    static boolean isTransient(Throwable failure) {
        if (!(failure instanceof QueryException)) {
            return false;
        }
        byte status = ((QueryException) failure).getStatus();
        return status == 0
                || status == WebSocketResponse.STATUS_INTERNAL_ERROR
                || status == QwpConstants.STATUS_LIMIT_EXCEEDED
                || status == QwpConstants.STATUS_CANCELLED;
    }

    /**
     * The task's conf string plus connect_timeout and auth_timeout_ms of {@code timeoutMs}
     * where it does not set them. The query client connects when a query is borrowed, before
     * the query timeout starts: without these, a host that drops packets blocks a load for the
     * OS connect timeout (over two minutes on Linux) and an unanswered WebSocket upgrade for
     * the client's 15 s default. The string is rebuilt rather than appended to, because a
     * trailing ';;' is an escaped semicolon inside the last value.
     */
    static String withQueryTimeouts(String confString, long timeoutMs) {
        StringSink token = new StringSink();
        int pos = ConfStringParser.of(confString, token);
        if (pos < 0) {
            return confString; // malformed: the client reports it
        }
        StringBuilder rebuilt = new StringBuilder(confString.length() + 64).append(token).append("::");
        boolean hasConnectTimeout = false;
        boolean hasAuthTimeout = false;
        while (ConfStringParser.hasNext(confString, pos)) {
            pos = ConfStringParser.nextKey(confString, pos, token);
            if (pos < 0) {
                return confString;
            }
            hasConnectTimeout |= Chars.equals(token, "connect_timeout");
            hasAuthTimeout |= Chars.equals(token, "auth_timeout_ms");
            rebuilt.append(token).append('=');
            pos = ConfStringParser.value(confString, pos, token);
            if (pos < 0) {
                return confString;
            }
            for (int i = 0, n = token.length(); i < n; i++) {
                char c = token.charAt(i);
                rebuilt.append(c);
                if (c == ';') {
                    rebuilt.append(';');
                }
            }
            rebuilt.append(';');
        }
        if (!hasConnectTimeout) {
            rebuilt.append("connect_timeout=").append(Math.min(timeoutMs, Integer.MAX_VALUE)).append(';');
        }
        if (!hasAuthTimeout) {
            rebuilt.append("auth_timeout_ms=").append(timeoutMs).append(';');
        }
        return rebuilt.toString();
    }

    void close() {
        QuestDB closing = db;
        db = null;
        if (closing != null) {
            closing.close();
        }
    }

    /**
     * One full load. Throws {@link QueryException} as the client does, with status 0 for a
     * timeout, and {@link ConnectException} when the result does not have the required shape.
     * <p>
     * Any failure discards the handle, so the next load starts from a fresh one. A pooled query
     * client can stay broken: with failover=off or failover_max_attempts=1 it reports a lost
     * connection on every later query. Failures are rare and a rebuild costs one connect.
     */
    Lookup load() {
        try {
            return loadOnce();
        } catch (RuntimeException e) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private Lookup loadOnce() {
        if (db == null) {
            db = QuestDB.builder()
                    .fromConfig(confString)
                    .senderPoolMin(0) // the task has its own Sender; do not open another
                    .queryPoolMin(0)  // connect on first borrow, not in build()
                    .queryPoolMax(1)
                    .build();
        }
        Loader loader = new Loader();
        try (Query query = db.borrowQuery()) {
            if (!query.sql(sql).handler(loader).submit().await(queryTimeoutMs, TimeUnit.MILLISECONDS)) {
                // Query.close() cancels and waits, bounded by the client's query_close_timeout_ms.
                // The loader may run until then; it is local and never read after this point.
                throw new QueryException((byte) 0, "enrichment query timed out after " + queryTimeoutMs + " ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryException((byte) 0, "interrupted while loading the enrichment lookup", e);
        }
        if (loader.error != null) {
            throw new ConnectException(loader.error);
        }
        return new Lookup(loader.columns, loader.map);
    }

    static final class Lookup {
        /** Names of the value columns; null when the query returned no batch at all. */
        final String[] columns;
        /** One slot per value column; a null slot is no value for that column. */
        final Map<String, String[]> map;

        Lookup(String[] columns, Map<String, String[]> map) {
            this.columns = columns;
            this.map = map;
        }
    }

    /**
     * Runs on a client worker thread. The loading thread reads it only after await() returned
     * true, which the client signals under a lock, so the fields need no synchronisation.
     */
    static final class Loader implements QwpColumnBatchHandler {
        String[] columns;
        String error;
        final Map<String, String[]> map = new HashMap<>();

        @Override
        public void onBatch(QwpColumnBatch batch) {
            if (error != null) {
                return;
            }
            int valueCount = batch.getColumnCount() - 1;
            if (valueCount < 1) {
                error = "enrichment.sql must return the lookup key and at least one value column";
                return;
            }
            // The typed accessors read native memory unchecked: never call one the wire type
            // does not match.
            byte keyType = batch.getColumnWireType(0);
            if (!isText(keyType) && !isInteger(keyType)) {
                error = "enrichment.sql key column '" + batch.getColumnName(0)
                        + "' must be VARCHAR, STRING, SYMBOL or an integer type; cast it in the SQL, for example "
                        + batch.getColumnName(0) + "::varchar";
                return;
            }
            String[] names = new String[valueCount];
            for (int col = 1; col <= valueCount; col++) {
                if (!isText(batch.getColumnWireType(col))) {
                    error = "enrichment.sql value column '" + batch.getColumnName(col)
                            + "' must be VARCHAR, STRING or SYMBOL; cast it in the SQL and give the cast an alias"
                            + " (...::varchar AS another_name)";
                    return;
                }
                names[col - 1] = batch.getColumnName(col);
            }
            columns = names;
            for (int row = 0, n = batch.getRowCount(); row < n; row++) {
                if (batch.isNull(0, row)) {
                    continue;
                }
                String[] values = null;
                for (int col = 1; col <= valueCount; col++) {
                    if (!batch.isNull(col, row)) {
                        if (values == null) {
                            values = new String[valueCount];
                        }
                        values[col - 1] = batch.getString(col, row);
                    }
                }
                String key;
                switch (keyType) {
                    case QwpConstants.TYPE_LONG:
                        key = Long.toString(batch.getLongValue(0, row));
                        break;
                    case QwpConstants.TYPE_INT:
                        key = Long.toString(batch.getIntValue(0, row));
                        break;
                    case QwpConstants.TYPE_SHORT:
                        key = Long.toString(batch.getShortValue(0, row));
                        break;
                    case QwpConstants.TYPE_BYTE:
                        key = Long.toString(batch.getByteValue(0, row));
                        break;
                    default:
                        key = batch.getString(0, row);
                        break;
                }
                // the last row for a key wins, also when it has no value at all
                if (values == null) {
                    map.remove(key);
                } else {
                    map.put(key, values);
                }
            }
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            // The statement has run once; failing the load keeps it from running on every refresh.
            error = "enrichment.sql must be a query returning the lookup key and value columns, not a statement";
        }

        @Override
        public void onError(byte status, String message) {
            // await() rethrows it as QueryException
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            // the query is replayed from the start on another endpoint
            map.clear();
        }

        private static boolean isInteger(byte wireType) {
            return wireType == QwpConstants.TYPE_LONG || wireType == QwpConstants.TYPE_INT
                    || wireType == QwpConstants.TYPE_SHORT || wireType == QwpConstants.TYPE_BYTE;
        }

        private static boolean isText(byte wireType) {
            // STRING columns arrive as VARCHAR on the wire
            return wireType == QwpConstants.TYPE_VARCHAR || wireType == QwpConstants.TYPE_SYMBOL;
        }
    }
}

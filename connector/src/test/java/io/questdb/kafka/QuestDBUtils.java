package io.questdb.kafka;

import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public final class QuestDBUtils {
    public enum Endpoint {
        EXPORT("exp"),
        EXEC("exec");

        private String endpoint;

        Endpoint(String endpoint) {
            this.endpoint = endpoint;
        }


        String getEndpoint() {
            return endpoint;
        }
    }


    public static final int QUESTDB_ILP_PORT = 9009;
    public static final int QUESTDB_HTTP_PORT = 9000;

    private static final int QUERY_WAITING_TIME_SECONDS = 30;
    private static final OkHttpClient CLIENT = new OkHttpClient(new OkHttpClient.Builder().connectionSpecs(Collections.singletonList(ConnectionSpec.CLEARTEXT)));

    private QuestDBUtils() {

    }

    public static void dropTableIfExists(String table, int port) {
        try (Response response = executeQuery(port, dropStatement(table), Endpoint.EXEC)) {
            if (response.code() != 200) {
                fail("Failed to drop table " + table + ", returned code " + response.code());
            }
        } catch (IOException e) {
            fail("Failed to drop table " + table, e);
        }
    }

    public static void awaitReady(int port) {
        assertSqlEventually("\"ready\"\r\n1\r\n", "select 1 ready", port);
    }

    /**
     * Drops every table, best-effort. Tests share one QuestDB instance per class
     * and QuestDB preallocates tens of megabytes per table, so keeping every
     * test's tables around grows the data directory to ~17GB per class run -
     * enough to exhaust the disk of a CI runner and slow everything to a crawl.
     */
    public static void dropAllTables(int port) {
        String csv;
        try (Response response = executeQuery(port, "select table_name from tables()", Endpoint.EXPORT)) {
            if (response.code() != 200) {
                return;
            }
            try (okhttp3.ResponseBody body = response.body()) {
                csv = body == null ? "" : body.string();
            }
        } catch (IOException e) {
            return; // QuestDB may be intentionally stopped by the test
        }
        String[] lines = csv.split("\r?\n");
        for (int i = 1; i < lines.length; i++) { // line 0 is the CSV header
            String name = lines[i].trim();
            if (name.startsWith("\"") && name.endsWith("\"") && name.length() > 1) {
                name = name.substring(1, name.length() - 1);
            }
            if (name.isEmpty()) {
                continue;
            }
            try (Response ignored = executeQuery(port, dropStatement(name), Endpoint.EXEC)) {
                // best-effort
            } catch (IOException e) {
                return;
            }
        }
    }

    private static String dropStatement(String table) {
        // quoted: several tests use table names with dots
        return "drop table if exists \"" + table + "\"";
    }

    public static void assertSqlEventually(String expectedResult, String query, int timeoutSeconds, int port) {
        await().pollInterval(250, TimeUnit.MILLISECONDS).atMost(timeoutSeconds, TimeUnit.SECONDS).untilAsserted(() -> assertSql(expectedResult, query, port));
    }

    public static void assertSqlEventually(String expectedResult, String query, int port) {
        await().atMost(QUERY_WAITING_TIME_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertSql(expectedResult, query, port));
    }

    public static void assertSql(String expectedResult, String query, int port) {
        assertSql(expectedResult, query, port, Endpoint.EXPORT);
    }

    public static void assertSql(String expectedResult, String query, int port, Endpoint endpoint) {
        try (Response response = executeQuery(port, query, endpoint)) {
            if (response.code() != 200) {
                fail("Query failed, returned code " + response.code());
            }
            try (okhttp3.ResponseBody body = response.body()) {
                if (body != null) {
                    String bodyString = body.string();
                    try {
                        assertEquals(expectedResult, bodyString);
                    } catch (AssertionError e) {
                        System.out.println("Received response: " + bodyString);
                        throw e;
                    }
                }
            }
        } catch (IOException e) {
            fail("Query failed", e);
        }
    }

    private static Response executeQuery(int port, String query, Endpoint endpoint) throws IOException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        String baseUrl = "http://localhost:" + port;
        Request request = new Request.Builder()
                .url(baseUrl + "/" + endpoint.endpoint + "?query=" + encodedQuery)
                .build();
        return CLIENT.newCall(request).execute();
    }
}

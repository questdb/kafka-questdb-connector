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

    public static void assertSqlEventually(String expectedResult, String query, int timeoutSeconds, int port) {
        await().pollInterval(5, TimeUnit.SECONDS).atMost(timeoutSeconds, TimeUnit.SECONDS).untilAsserted(() -> assertSql(expectedResult, query, port));
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
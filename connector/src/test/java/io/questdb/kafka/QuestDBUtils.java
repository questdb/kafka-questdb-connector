package io.questdb.kafka;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public final class QuestDBUtils {
    public static final int QUESTDB_ILP_PORT = 9009;
    public static final int QUESTDB_HTTP_PORT = 9000;

    private static final int QUERY_WAITING_TIME_SECONDS = 30;
    private static final OkHttpClient CLIENT = new OkHttpClient();

    private QuestDBUtils() {

    }

    static void assertSqlEventually(GenericContainer<?> questdbContainer, String expectedResult, String query) {
        await().atMost(QUERY_WAITING_TIME_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertSql(questdbContainer, expectedResult, query));
    }

    private static void assertSql(GenericContainer<?> questdbContainer, String expectedResult, String query) throws IOException {
        try (Response response = executeQuery(questdbContainer, query)) {
            if (response.code() != 200) {
                fail("Query failed, returned code " + response.code());
            }
            try (okhttp3.ResponseBody body = response.body()) {
                if (body != null) {
                    String bodyString = body.string();
                    assertEquals(expectedResult, bodyString);
                }
            }
        }
    }

    private static Response executeQuery(GenericContainer<?> questContainer, String query) throws IOException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        String baseUrl = "http://" + questContainer.getHost() + ":" + questContainer.getMappedPort(QUESTDB_HTTP_PORT);
        Request request = new Request.Builder()
                .url(baseUrl + "/exp?query=" + encodedQuery)
                .build();
        return CLIENT.newCall(request).execute();
    }

}
package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Transport selection must happen on the worker that actually runs the task:
 * Kafka Connect stamps the task class into the task configuration on the worker
 * hosting the connector, but the tasks may execute elsewhere, and
 * {@code QDB_CLIENT_CONF} is worker-local. This cluster gives each worker a
 * different environment - one {@code ws::} (QWP), one {@code http::} - and both
 * must run the implementation matching their own environment.
 */
@Testcontainers
public class MultiWorkerTransportIT {
    private static final String QWP_WORKER = "connect-qwp";
    private static final String HTTP_WORKER = "connect-http";
    private static final String QWP_TASK_MARKER = "Starting QuestDB QWP sink task";
    private static final String LEGACY_TASK_MARKER = "Starting QuestDB sink task";

    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbJarResolver = JarResolverExtension.forClass(Sender.class);

    private final static Network network = Network.newNetwork();

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withKraft()
            .withEnv("KAFKA_BROKER_ID", "0")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "0@kafka:9094");

    @Container
    private static final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:10.0.0")
            .withNetwork(network)
            .withNetworkAliases("questdb")
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")));

    @Container
    private static final GenericContainer<?> qwpWorker = newWorker(QWP_WORKER, "ws::addr=questdb:9000;");

    @Container
    private static final GenericContainer<?> httpWorker = newWorker(HTTP_WORKER, "http::addr=questdb:9000;");

    private static GenericContainer<?> newWorker(String name, String clientConf) {
        return new GenericContainer<>("confluentinc/cp-kafka-connect:7.8.0")
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
                // one group: both workers form a single distributed cluster
                .withEnv("CONNECT_GROUP_ID", "multi-worker-transport")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-storage-topic")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config-topic")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status-topic")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", name)
                // the whole point: worker-local transport configuration
                .withEnv("QDB_CLIENT_CONF", clientConf)
                .withNetwork(network)
                .withNetworkAliases(name)
                .withExposedPorts(8083)
                .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/usr/share/java/kafka/questdb-connector.jar")
                .withCopyFileToContainer(MountableFile.forHostPath(questdbJarResolver.getJarPath()), "/usr/share/java/kafka/questdb.jar")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(name)))
                .dependsOn(kafkaContainer, questDBContainer)
                .waitingFor(new HttpWaitStrategy()
                        .forPath("/connectors")
                        .forStatusCode(200)
                        .forPort(8083)
                        .withStartupTimeout(ofMinutes(5)));
    }

    @Test
    public void testEachWorkerResolvesItsOwnTransport() throws Exception {
        String topicName = "multi_worker_topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 20; i++) {
                producer.send(new ProducerRecord<>(topicName, "key" + i, "{\"id\":" + i + "}")).get();
            }
        }

        // no client.conf.string: each worker must resolve QDB_CLIENT_CONF locally
        String payload = "{\"name\":\"multi-worker-connector\",\"config\":{"
                + "\"connector.class\":\"io.questdb.kafka.QuestDBSinkConnector\","
                + "\"tasks.max\":\"4\","
                + "\"key.converter\":\"org.apache.kafka.connect.storage.StringConverter\","
                + "\"value.converter\":\"org.apache.kafka.connect.json.JsonConverter\","
                + "\"value.converter.schemas.enable\":\"false\","
                + "\"include.key\":\"false\","
                + "\"topics\":\"" + topicName + "\"}}";

        HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(ofSeconds(10)).build().send(
                HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString(payload))
                        .uri(new URI("http://localhost:" + httpWorker.getMappedPort(8083) + "/connectors"))
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new IllegalStateException("Failed to create connector: " + response.body());
        }

        QuestDBUtils.assertSqlEventually("\"count()\"\r\n20\r\n",
                "select count() from " + topicName,
                120,
                questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));

        String qwpLog = qwpWorker.getLogs();
        String httpLog = httpWorker.getLogs();

        // Each worker runs the implementation its own environment asks for. The
        // regression this guards: selecting the task class on the connector's
        // worker would run the legacy task here against a ws:: config.
        assertFalse(qwpLog.contains(LEGACY_TASK_MARKER),
                "worker with ws:: config must not start the legacy task");
        assertFalse(httpLog.contains(QWP_TASK_MARKER),
                "worker with http:: config must not start the QWP task");
        assertTrue(qwpLog.contains(QWP_TASK_MARKER) || httpLog.contains(LEGACY_TASK_MARKER),
                "at least one worker must have started a sink task");
    }
}

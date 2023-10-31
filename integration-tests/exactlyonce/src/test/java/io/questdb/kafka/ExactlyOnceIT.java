package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.std.Os;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class ExactlyOnceIT {
    private static final int VICTIM_QUESTDB = 0;
    private static final int VICTIM_CONNECT = 1;
    private static final int VICTIM_KAFKA = 2;
    private static final int VICTIMS_TOTAL = VICTIM_KAFKA + 1;

    private static final DockerImageName KAFKA_CONTAINER_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.5.1");
    private static final DockerImageName ZOOKEEPER_CONTAINER_IMAGE = DockerImageName.parse("confluentinc/cp-zookeeper:7.5.1");
    private static final DockerImageName CONNECT_CONTAINER_IMAGE = DockerImageName.parse("confluentinc/cp-kafka-connect:7.5.1");
    private static final DockerImageName QUESTDB_CONTAINER_IMAGE = DockerImageName.parse("questdb/questdb:7.3.3");
    private static final int KAFKA_CLUSTER_SIZE = 3;
    private static final int CONNECT_CLUSTER_SIZE = 2;

    @TempDir(cleanup = CleanupMode.NEVER)
    static Path persistence;

    // we need to locate JARs with QuestDB client and Kafka Connect Connector,
    // this is later used to copy to the Kafka Connect container
    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbClientJarResolver = JarResolverExtension.forClass(Sender.class);

    private final static Network network = Network.newNetwork();

    private static GenericContainer<?> zookeeper;
    private static KafkaContainer[] kafkas = new KafkaContainer[KAFKA_CLUSTER_SIZE];
    private static GenericContainer[] connects = new GenericContainer[CONNECT_CLUSTER_SIZE];
    private static GenericContainer<?> questdb;

    private static int questHttpPort;

    @BeforeAll
    public static void createContainers() {
        zookeeper = newZookeeperContainer();
        questdb = newQuestDBContainer();
        for (int i = 0; i < KAFKA_CLUSTER_SIZE; i++) {
            kafkas[i] = newKafkaContainer(i);
        }
        for (int i = 0; i < CONNECT_CLUSTER_SIZE; i++) {
            connects[i] = newConnectContainer(i);
        }

        Stream<Startable> containers = Stream.concat(
                Stream.concat(
                        Stream.of(kafkas), Stream.of(connects)
                ),
                Stream.of(zookeeper, questdb)
        );
        Startables.deepStart(containers).join();
        questHttpPort = questdb.getMappedPort(9000);
    }

    @AfterAll
    public static void stopContainer() {
        questdb.stop();
        Stream.of(kafkas).forEach(KafkaContainer::stop);
        Stream.of(connects).forEach(GenericContainer::stop);
        zookeeper.stop();

        io.questdb.std.Files.rmdir(io.questdb.std.str.Path.getThreadLocal(persistence.toAbsolutePath().toString()));
    }

    private static GenericContainer<?> newZookeeperContainer() {
         return new GenericContainer<>(ZOOKEEPER_CONTAINER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "300")
                .withEnv("ZOOKEEPER_INIT_LIMIT", "10")
                .withEnv("ZOOKEEPER_SYNC_LIMIT", "5")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("zookeeper")));
    }

    private static GenericContainer<?> newQuestDBContainer() {

        Path dbRoot;
        try {
            dbRoot = Files.createDirectories(persistence.resolve("questdb"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(QUESTDB_CONTAINER_IMAGE.asCanonicalNameString())
                .withNetwork(network)
//                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
                .withFileSystemBind(dbRoot.toAbsolutePath().toString(), "/var/lib/questdb")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("questdb"));

        if (questHttpPort == 0) {
            container = container.withExposedPorts(9000);
        } else {
            container.withFixedExposedPort(questHttpPort, 9000);
        }
        return container;
    }

    private static KafkaContainer newKafkaContainer(int id) {
        Path kafkaData;
        try {
            kafkaData = Files.createDirectories(persistence.resolve("kafka").resolve("data" + id));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new KafkaContainer(KAFKA_CONTAINER_IMAGE)
                .withNetwork(network)
                .dependsOn(zookeeper)
                .withExternalZookeeper("zookeeper:2181")
                .withEnv("KAFKA_BROKER_ID", String.valueOf(id))
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
                .withEnv("KAFKA_NUM_PARTITIONS", "3")
                .withFileSystemBind(kafkaData.toAbsolutePath().toString(), "/var/lib/kafka/data")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka" + id))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka" + id)));
    }

    private static GenericContainer<?> newConnectContainer(int id) {
        List<Startable> dependencies = new ArrayList<>(Arrays.asList(kafkas));
        dependencies.add(questdb);

        return new GenericContainer<>(CONNECT_CONTAINER_IMAGE)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka0:9092")
                .withEnv("CONNECT_GROUP_ID", "test")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-storage-topic")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config-topic")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status-topic")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect" + id)
                .withNetwork(network)
                .withExposedPorts(8083)
                .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/usr/share/java/kafka/questdb-connector.jar")
                .withCopyFileToContainer(MountableFile.forHostPath(questdbClientJarResolver.getJarPath()), "/usr/share/java/kafka/questdb.jar")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("connect" + id)))
                .dependsOn(dependencies)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("connect" + id))
                .waitingFor(new HttpWaitStrategy()
                        .forPath("/connectors")
                        .forStatusCode(200)
                        .forPort(8083)
                        .withStartupTimeout(ofMinutes(5)));
    }

    @Test
    public void test() throws Exception {
        String topicName = "mytopic";

        Properties props = new Properties();
        String bootstrapServers = kafkas[0].getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("include.key", "false");

        int recordCount = 5_000_000;
        new Thread(() -> {
            try (Producer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < recordCount; i++ ) {
                    String json = newPayload();
                    producer.send(new ProducerRecord<>(topicName, null, json));

                    // 1% chance of duplicates - we want them to be also deduped by QuestDB
                    if (ThreadLocalRandom.current().nextInt(100) == 0) {
                        producer.send(new ProducerRecord<>(topicName, null, json));
                    }
                }
            }
        }).start();

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "CREATE TABLE " + topicName + " (ts TIMESTAMP, id UUID, val LONG) timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, id);",
                questdb.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT),
                QuestDBUtils.Endpoint.EXEC);

        startConnector();

        CyclicBarrier barrier = new CyclicBarrier(2);
        startKillingRandomContainers(barrier);

        // make sure we have exactly the expected records in QuestDB
        QuestDBUtils.assertSqlEventually(
                "\"count\"\r\n"
                        + recordCount + "\r\n",
                "select count(*) from " + topicName,
                600,
                questHttpPort);

        barrier.await();
    }

    @NotNull
    private static String newPayload() {
        Instant now = Instant.now();
        long nanoTs = now.getEpochSecond() * 1_000_000_000 + now.getNano();
        UUID uuid = UUID.randomUUID();
        int val = ThreadLocalRandom.current().nextInt(100);

        return "{\"ts\":" + nanoTs + ",\"id\":\"" + uuid + "\",\"val\":" + val + "}";
    }

    private static void startKillingRandomContainers(CyclicBarrier barrier) {
        new Thread(() -> {
            while (barrier.getNumberWaiting() == 0) { // keep killing them until the checker thread passed the assertion
                Os.sleep(ThreadLocalRandom.current().nextInt(5_000, 30_000));
                int victim = ThreadLocalRandom.current().nextInt(VICTIMS_TOTAL);
                switch (victim) {
                    case VICTIM_QUESTDB: {
                        questdb.stop();
                        GenericContainer<?> container = newQuestDBContainer();
                        container.start();
                        questdb = container;
                        break;
                    }
                    case VICTIM_CONNECT: {
                        int n = ThreadLocalRandom.current().nextInt(connects.length);
                        connects[n].stop();
                        GenericContainer<?> container = newConnectContainer(n);
                        container.start();
                        connects[n] = container;
                        break;
                    }
                    case VICTIM_KAFKA: {
                        int n = ThreadLocalRandom.current().nextInt(kafkas.length);
                        kafkas[n].stop();
                        KafkaContainer container = newKafkaContainer(n);
                        Os.sleep(5000); // wait for zookeeper to detect the previous kafka container was stopped
                        container.start();
                        kafkas[n] = container;
                        break;
                    }
                }
            }

            try {
                barrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private static void startConnector() throws IOException, InterruptedException, URISyntaxException {
        String payload = "{\"name\":\"my-connector\",\"config\":{" +
                "\"tasks.max\":\"4\"," +
                "\"connector.class\":\"io.questdb.kafka.QuestDBSinkConnector\"," +
                "\"key.converter\":\"org.apache.kafka.connect.storage.StringConverter\"," +
                "\"value.converter\":\"org.apache.kafka.connect.json.JsonConverter\"," +
                "\"topics\":\"mytopic\"," +
                "\"value.converter.schemas.enable\":\"false\"," +
                "\"dedup.rewind.offset\":\"150000\"," +
                "\"timestamp.field.name\":\"ts\"," +
                "\"host\":\"questdb:9009\"}" +
                "}";

        HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(ofSeconds(10)).build().send(
                HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString(payload))
                        .uri(new URI("http://localhost:" + connects[0].getMappedPort(8083) + "/connectors"))
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        if (response.statusCode() != 201) {
            throw new RuntimeException("Failed to create connector: " + response.body());
        }
    }
}

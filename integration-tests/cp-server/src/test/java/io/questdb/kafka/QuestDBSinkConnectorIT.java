package io.questdb.kafka;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
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

import java.util.Properties;

import static java.time.Duration.ofMinutes;

@Testcontainers
public class QuestDBSinkConnectorIT {

    // we need to locate JARs with QuestDB client and Kafka Connect Connector,
    // this is later used to copy to the Kafka Connect container
    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbJarResolver = JarResolverExtension.forClass(Sender.class);

    private final static Network network = Network.newNetwork();

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
            .withNetwork(network);

    @Container
    private static final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:6.5.3")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @Container
    private static final DebeziumContainer connectContainer = new DebeziumContainer("confluentinc/cp-kafka-connect:7.2.1")
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.getNetworkAliases().get(0) + ":9092")
            .withEnv("CONNECT_GROUP_ID", "test")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-storage-topic")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config-topic")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status-topic")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withNetwork(network)
            .withExposedPorts(8083)
            .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/usr/share/java/kafka/questdb-connector.jar")
            .withCopyFileToContainer(MountableFile.forHostPath(questdbJarResolver.getJarPath()), "/usr/share/java/kafka/questdb.jar")
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("connect")))
            .dependsOn(kafkaContainer, questDBContainer)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/connectors")
                    .forStatusCode(200)
                    .forPort(8083)
                    .withStartupTimeout(ofMinutes(5)));

    @Test
    public void test() throws Exception {
        String topicName = "mytopic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topicName, "foo", "bar")).get();
        }

        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", QuestDBSinkConnector.class.getName())
                .with("tasks.max", "1")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("topics", topicName)
                .with("host", questDBContainer.getNetworkAliases().get(0) + ":" + QuestDBUtils.QUESTDB_ILP_PORT);

        connectContainer.registerConnector("my-connector", connector);

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"value\"\r\n"
                + "\"foo\",\"bar\"\r\n", "select key, value from " + topicName);

    }
}

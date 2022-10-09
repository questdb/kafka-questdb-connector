package kafka;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.questdb.client.Sender;
import io.questdb.kafka.QuestDBSinkConnector;
import io.questdb.kafka.QuestDBSinkConnectorConfig;
import io.questdb.kafka.QuestDBSinkTask;
import io.questdb.kafka.QuestDBUtils;
import io.questdb.kafka.JarResolverExtension;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;

@Testcontainers
public class DebeziumIT {

    // we need to locate JARs with QuestDB client and Kafka Connect Connector,
    // this is later used to copy to the Kafka Connect container
    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbJarResolver = JarResolverExtension.forClass(Sender.class);

    private static Network network = Network.newNetwork();

    private static KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network);

    public static PostgreSQLContainer<?> postgresContainer =
            new PostgreSQLContainer<>(DockerImageName.parse("debezium/postgres:11").asCompatibleSubstituteFor("postgres"))
                    .withNetwork(network)
                    .withNetworkAliases("postgres");

    public static DebeziumContainer debeziumContainer =
            new DebeziumContainer("debezium/connect:1.9.6.Final")
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/kafka/connect/questdb-connector/questdb-connector.jar")
                    .withCopyFileToContainer(MountableFile.forHostPath(questdbJarResolver.getJarPath()), "/kafka/connect/questdb-connector/questdb.jar")
                    .dependsOn(kafkaContainer)
                    .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "true")
                    .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "true");

    @Container
    private static final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:6.5.3")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @BeforeAll
    public static void startContainers() {
        Startables.deepStart(Stream.of(
                        kafkaContainer, postgresContainer, debeziumContainer))
                .join();
    }

    @Test
    public void testSmoke() throws Exception {
        String tableName = "todo";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, " +
                    "title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, " +
                    "'Learn CDC')");
            statement.execute("insert into todo.Todo values (2, " +
                    "'Learn Debezium')");

            ConnectorConfiguration connector = ConnectorConfiguration
                    .forJdbcContainer(postgresContainer)
                    .with("database.server.name", "dbserver1");


            debeziumContainer.registerConnector("my-connector", connector);

            ConnectorConfiguration questSink = ConnectorConfiguration.create()
                    .with("connector.class", QuestDBSinkConnector.class.getName())
                    .with("host", questDBContainer.getNetworkAliases().get(0))
                    .with("tasks.max", "1")
                    .with("topics", "dbserver1.todo.todo")
                    .with(QuestDBSinkConnectorConfig.TABLE_CONFIG, tableName)
                    .with("key.converter", JsonConverter.class.getName())
                    .with("value.converter", JsonConverter.class.getName())
                    .with("transforms", "unwrap")
                    .with("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                    .with(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");

            debeziumContainer.registerConnector("questdb-sink", questSink);


            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                    + "1,\"Learn CDC\"\r\n"
                    + "2,\"Learn Debezium\"\r\n", "select id, title from " + tableName);
        }
    }

    private static Connection getConnection(
            PostgreSQLContainer<?> postgresContainer)
            throws SQLException {

        return DriverManager.getConnection(postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }
}

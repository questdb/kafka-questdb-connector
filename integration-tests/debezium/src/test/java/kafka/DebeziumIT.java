package kafka;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.questdb.client.Sender;
import io.questdb.kafka.QuestDBSinkConnector;
import io.questdb.kafka.QuestDBSinkConnectorConfig;
import io.questdb.kafka.QuestDBSinkTask;
import io.questdb.kafka.QuestDBUtils;
import io.questdb.kafka.JarResolverExtension;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;

@Testcontainers
public class DebeziumIT {
    private static final String PG_SCHEMA_NAME = "test";
    private static final String PG_TABLE_NAME = "test";
    private static final String PG_SERVER_NAME = "dbserver1";
    private static final String DEBEZIUM_CONNECTOR_NAME = "debezium_source";
    private static final String QUESTDB_CONNECTOR_NAME = "questdb_sink";

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
//                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("debezium")))
                    .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "true")
                    .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "true");

    @Container
    private static final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:6.5.3")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
//            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @BeforeAll
    public static void startContainers() {
        Startables.deepStart(Stream.of(
                        kafkaContainer, postgresContainer, debeziumContainer))
                .join();
    }

    @AfterEach
    public void cleanup() throws SQLException {
        debeziumContainer.deleteAllConnectors();
        try (Connection connection = getConnection(postgresContainer);
            Statement statement = connection.createStatement()) {
            statement.execute("drop schema " + PG_SCHEMA_NAME + " CASCADE");
        }
    }

    private static ConnectorConfiguration newQuestSinkBaseConfig(String questTableName) {
        ConnectorConfiguration questSink = ConnectorConfiguration.create()
                .with("connector.class", QuestDBSinkConnector.class.getName())
                .with("host", questDBContainer.getNetworkAliases().get(0))
                .with("tasks.max", "1")
                .with("topics", PG_SERVER_NAME + "."+ PG_SCHEMA_NAME + "." + PG_TABLE_NAME)
                .with(QuestDBSinkConnectorConfig.TABLE_CONFIG, questTableName)
                .with("key.converter", JsonConverter.class.getName())
                .with("value.converter", JsonConverter.class.getName())
                .with("transforms", "unwrap")
                .with("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                .with(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        return questSink;
    }

    @Test
    public void testSmoke() throws Exception {
        String questTableName = "test_smoke";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " replica identity full");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC')");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (2, 'Learn Debezium')");
            startDebeziumConnector();

            ConnectorConfiguration questSinkConfig = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSinkConfig);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                    + "1,\"Learn CDC\"\r\n"
                    + "2,\"Learn Debezium\"\r\n",
                    "select id, title from " + questTableName);
        }
    }

    @Test
    public void testSchemaChange() throws Exception {
        String questTableName = "test_schema_change";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " replica identity full");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC')");

            startDebeziumConnector();
            
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                            + "1,\"Learn CDC\"\r\n",
                    "select id, title from " + questTableName);

            statement.execute("alter table "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " add column description varchar(255)");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (2, 'Learn Debezium', 'Best book ever')");

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"description\"\r\n"
                    + "1,\"Learn CDC\",\r\n"
                    + "2,\"Learn Debezium\",\"Best book ever\"\r\n",
                    "select id, title, description from " + questTableName);
        }
    }

    @Test
    public void testUpdatesChange() throws Exception {
        String questTableName = "test_updates_change";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " replica identity full");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                            + "1,\"Learn CDC\"\r\n",
                    "select id, title from " + questTableName);

            statement.executeUpdate("update "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " set title = 'Learn Debezium' where id = 1");

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                            + "1,\"Learn CDC\"\r\n"
                            + "1,\"Learn Debezium\"\r\n",
                    "select id, title from " + questTableName);
        }
    }

    @Test
    public void testInsertThenDeleteThenInsertAgain() throws Exception {
        String questTableName = "test_insert_then_delete_then_insert_again";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " replica identity full");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                            + "1,\"Learn CDC\"\r\n",
                    "select id, title from " + questTableName);

            statement.execute("delete from "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " where id = 1");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn Debezium')");

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\"\r\n"
                            + "1,\"Learn CDC\"\r\n"
                            + "1,\"Learn Debezium\"\r\n",
                    "select id, title from " + questTableName);
        }
    }

    @Test
    public void testEventTime() throws SQLException {
        String questTableName = "test_event_time";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at timestamp, primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02T01:02:03.456Z')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            questSink.with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "created_at");
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"timestamp\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T01:02:03.456000Z\"\r\n",
                    "select id, title, timestamp from " + questTableName);
        }
    }

    @Test
    public void testNonDesignatedTimestamp() throws SQLException {
        String questTableName = "test_non_designated_timestamp";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at timestamp, primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02T01:02:03.456Z')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"created_at\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T01:02:03.456000Z\"\r\n",
                    "select id, title, created_at from " + questTableName);
        }
    }

    private static void startDebeziumConnector() {
        ConnectorConfiguration connector = ConnectorConfiguration
                .forJdbcContainer(postgresContainer)
                .with("database.server.name", PG_SERVER_NAME);
        debeziumContainer.registerConnector(DEBEZIUM_CONNECTOR_NAME, connector);
    }


    private static Connection getConnection(
            PostgreSQLContainer<?> postgresContainer)
            throws SQLException {

        return DriverManager.getConnection(postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }
}

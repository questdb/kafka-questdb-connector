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
import org.junit.jupiter.api.Assertions;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

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

    private static final Network network = Network.newNetwork();

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
            .withNetwork(network);

    @Container
    public PostgreSQLContainer<?> postgresContainer =
            new PostgreSQLContainer<>(DockerImageName.parse("debezium/postgres:11-alpine").asCompatibleSubstituteFor("postgres"))
                    .withNetwork(network)
                    .withNetworkAliases("postgres");

    @Container
    public DebeziumContainer debeziumContainer =
            new DebeziumContainer("debezium/connect:1.9.6.Final")
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/kafka/connect/questdb-connector/questdb-connector.jar")
                    .withCopyFileToContainer(MountableFile.forHostPath(questdbJarResolver.getJarPath()), "/kafka/connect/questdb-connector/questdb.jar")
                    .dependsOn(kafkaContainer)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("debezium")))
                    .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "true")
                    .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "true");

    @Container
    private final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:6.5.3")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");


    private ConnectorConfiguration newQuestSinkBaseConfig(String questTableName) {
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
    public void testManyUpdates() throws Exception {
        String questTableName = "test_many_updates";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {
            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            questSink = questSink.with("transforms.unwrap.add.fields", "source.ts_ms")
                            .with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "__source_ts_ms");
            questSink.with(QuestDBSinkConnectorConfig.SYMBOL_COLUMNS_CONFIG, "symbol");
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, symbol varchar(255), price double precision, primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (0, 'TDB', 1.0)");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'QDB', 1.0)");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (2, 'IDB', 1.0)");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (3, 'PDB', 1.0)");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (4, 'KDB', 1.0)");

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"symbol\",\"price\"\r\n"
                            + "0,\"TDB\",1.0\r\n"
                            + "1,\"QDB\",1.0\r\n"
                            + "2,\"IDB\",1.0\r\n"
                            + "3,\"PDB\",1.0\r\n"
                            + "4,\"KDB\",1.0\r\n",
                    "select id, symbol, price from " + questTableName);

            try (PreparedStatement preparedStatement = connection.prepareStatement("update " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " set price = ? where id = ?")) {
                //a bunch of updates
                for (int i = 0; i < 200_000; i++) {
                    int id = ThreadLocalRandom.current().nextInt(5);
                    double newPrice = ThreadLocalRandom.current().nextDouble(100);
                    preparedStatement.setDouble(1, newPrice);
                    preparedStatement.setInt(2, id);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                // set all prices to a known value, this will be useful in asserting the final state
                for (int i = 0; i < 5; i++) {
                    preparedStatement.setDouble(1, 42.0);
                    preparedStatement.setInt(2, i);
                    Assertions.assertEquals(1, preparedStatement.executeUpdate());
                }
            }

            // all symbols have the last well-known price
            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"symbol\",\"last_price\"\r\n"
                            + "0,\"TDB\",42.0\r\n"
                            + "1,\"QDB\",42.0\r\n"
                            + "2,\"IDB\",42.0\r\n"
                            + "3,\"PDB\",42.0\r\n"
                            + "4,\"KDB\",42.0\r\n",
                    "select id, symbol, last(price) as last_price from " + questTableName);

            // total number of rows is equal to the number of updates and inserts
            QuestDBUtils.assertSqlEventually(questDBContainer, "\"count\"\r\n"
                            + "200010\r\n",
                    "select count() from " + questTableName);
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
    public void testEventTimeMicros() throws SQLException {
        String questTableName = "test_event_time_micros";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at timestamp(6), primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02T01:02:03.123456Z')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            questSink.with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "created_at");
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"timestamp\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T01:02:03.123456Z\"\r\n",
                    "select id, title, timestamp from " + questTableName);
        }
    }

    @Test
    public void testEventTimeNanos() throws SQLException {
        String questTableName = "test_event_time_nanos";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at timestamp(9), primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02T01:02:03.123456789Z')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            questSink.with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "created_at");
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"timestamp\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T01:02:03.123457Z\"\r\n",
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

    @Test
    public void testDate() throws SQLException {
        String questTableName = "test_date";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at date, primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02')");

            startDebeziumConnector();
            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"created_at\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T00:00:00.000000Z\"\r\n",
                    "select id, title, created_at from " + questTableName);
        }
    }

    @Test
    public void testDelete() throws SQLException {
        String questTableName = "test_delete";
        try (Connection connection = getConnection(postgresContainer);
             Statement statement = connection.createStatement()) {
            startDebeziumConnector();

            statement.execute("create schema " + PG_SCHEMA_NAME);
            statement.execute("create table " + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " (id int8 not null, title varchar(255), created_at timestamp, primary key (id))");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (1, 'Learn CDC', '2021-01-02')");

            ConnectorConfiguration questSink = newQuestSinkBaseConfig(questTableName);
            questSink.with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "created_at");
            debeziumContainer.registerConnector(QUESTDB_CONNECTOR_NAME, questSink);

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"timestamp\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T00:00:00.000000Z\"\r\n",
                    "select * from " + questTableName);

            // delete should be ignored by QuestDB
            statement.execute("delete from "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " where id = 1");
            statement.execute("insert into "  + PG_SCHEMA_NAME + "." + PG_TABLE_NAME + " values (2, 'Learn Debezium', '2021-01-03')");

            QuestDBUtils.assertSqlEventually(questDBContainer, "\"id\",\"title\",\"timestamp\"\r\n"
                            + "1,\"Learn CDC\",\"2021-01-02T00:00:00.000000Z\"\r\n"
                            + "2,\"Learn Debezium\",\"2021-01-03T00:00:00.000000Z\"\r\n",
                    "select * from " + questTableName);
        }
    }

    private void startDebeziumConnector() {
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

package io.questdb.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.questdb.client.Sender;
import io.questdb.kafka.domain.SensorReading;
import io.questdb.kafka.domain.Student;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import static java.time.Duration.ofMinutes;

@Testcontainers
public class AvroSchemaRegistryIT {
    // we need to locate JARs with QuestDB client and Kafka Connect Connector,
    // this is later used to copy to the Kafka Connect container
    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbJarResolver = JarResolverExtension.forClass(Sender.class);

    private final static Network network = Network.newNetwork();

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withKraft()
            .withEnv("KAFKA_BROKER_ID", "0")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "0@kafka:9094");

    @Container
    private final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:9.0.1")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @Container
    private final DebeziumContainer connectContainer = new DebeziumContainer("confluentinc/cp-kafka-connect:7.6.0")
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
//            .withEnv("QDB_DEBUG", "true")
            .withNetwork(network)
            .withExposedPorts(8083)
            .withCopyFileToContainer(MountableFile.forHostPath(connectorJarResolver.getJarPath()), "/usr/share/java/kafka/questdb-connector.jar")
            .withCopyFileToContainer(MountableFile.forHostPath(questdbJarResolver.getJarPath()), "/usr/share/java/kafka/questdb.jar")
//            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("connect")))
            .dependsOn(kafkaContainer, questDBContainer)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/connectors")
                    .forStatusCode(200)
                    .forPort(8083)
                    .withStartupTimeout(ofMinutes(5)));

    @Container
    private GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.0"))
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.getNetworkAliases().get(0) + ":9092")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withExposedPorts(8081)
            .dependsOn(kafkaContainer)
            .waitingFor(Wait.forHttp("/subjects"));

    @Test
    public void testSmoke() throws Exception {
        String topicName = "mytopic";
        try (Producer<String, Student> producer = new KafkaProducer<>(producerProps())) {
            Student student = Student.newBuilder()
                    .setFirstname("John")
                    .setLastname("Doe")
                    .setBirthday(Instant.parse("2000-01-01T00:00:00Z"))
                    .build();
            producer.send(new ProducerRecord<>(topicName, "foo", student)).get();
        }

        startConnector(topicName, "birthday");
        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2000-01-01T00:00:00.000000Z\"\r\n",
                "select * from " + topicName, questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        String topicName = "mytopic";
        try (Producer<String, Student> producer = new KafkaProducer<>(producerProps())) {
            Student student = Student.newBuilder()
                    .setFirstname("John")
                    .setLastname("Doe")
                    .setBirthday(Instant.parse("2000-01-01T00:00:00Z"))
                    .build();
            producer.send(new ProducerRecord<>(topicName, "foo", student)).get();
        }
        startConnector(topicName, "birthday");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                + "\"John\",\"Doe\",\"2000-01-01T00:00:00.000000Z\"\r\n",
                "select * from " + topicName, questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));

        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProps())) {
            Schema schema = new org.apache.avro.Schema.Parser().parse(getClass().getResourceAsStream("/avro-runtime/StudentWithExtraColumn.avsc"));
            GenericRecord student = new GenericData.Record(schema);
            student.put("firstname", "Mary");
            student.put("lastname", "Doe");
            student.put("birthday", Instant.parse("2005-01-01T00:00:00Z").toEpochMilli());
            student.put("active", true);
            producer.send(new ProducerRecord<>(topicName, "foo", student)).get();
        }
        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"timestamp\",\"active\"\r\n"
                        + "\"John\",\"Doe\",\"2000-01-01T00:00:00.000000Z\",false\r\n"
                        + "\"Mary\",\"Doe\",\"2005-01-01T00:00:00.000000Z\",true\r\n",
                "select * from " + topicName, questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    @Test
    public void testAvroRecordsWithArrays() throws Exception {
        String topicName = "sensors";
        
        // sensor reading with array of double values
        try (Producer<String, SensorReading> producer = new KafkaProducer<>(producerProps())) {
            SensorReading reading = SensorReading.newBuilder()
                    .setSensorId("sensor-001")
                    .setTimestamp(Instant.parse("2024-01-01T10:00:00Z"))
                    .setValues(Arrays.asList(22.5, 23.1, 22.8, 23.3, 22.9))
                    .setLocation("Building A")
                    .build();
            producer.send(new ProducerRecord<>(topicName, "key1", reading)).get();
            
            // Send another reading
            SensorReading reading2 = SensorReading.newBuilder()
                    .setSensorId("sensor-002")
                    .setTimestamp(Instant.parse("2024-01-01T10:05:00Z"))
                    .setValues(Arrays.asList(18.2, 18.5, 18.3))
                    .setLocation(null)
                    .build();
            producer.send(new ProducerRecord<>(topicName, "key2", reading2)).get();
        }

        startConnector(topicName, "timestamp");

        QuestDBUtils.assertSqlEventually(
                "\"sensorId\",\"values\",\"location\",\"timestamp\"\r\n" +
                        "\"sensor-001\",\"[22.5,23.1,22.8,23.3,22.9]\",\"Building A\",\"2024-01-01T10:00:00.000000Z\"\r\n" +
                        "\"sensor-002\",\"[18.2,18.5,18.3]\",,\"2024-01-01T10:05:00.000000Z\"\r\n",
                "select sensorId, \"values\", location, timestamp from " + topicName + " order by timestamp",
                questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    private void startConnector(String topicName, String timestampName) {
        String confString = "http::addr=" + questDBContainer.getNetworkAliases().get(0) + ":" + QuestDBUtils.QUESTDB_HTTP_PORT + ";auto_flush_rows=1;";
        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", QuestDBSinkConnector.class.getName())
                .with("tasks.max", "1")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.avro.AvroConverter")
                .with("value.converter.schema.registry.url", "http://" + schemaRegistry.getNetworkAliases().get(0) + ":8081")
                .with("topics", topicName)
                .with(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, timestampName)
                .with(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false")
                .with("client.conf.string", confString);
        connectContainer.registerConnector("my-connector", connector);
    }

    @NotNull
    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());
        return props;
    }
}

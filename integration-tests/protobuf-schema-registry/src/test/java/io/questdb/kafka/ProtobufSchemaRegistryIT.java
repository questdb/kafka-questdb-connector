package io.questdb.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.questdb.client.Sender;
import io.questdb.kafka.domain.Address;
import io.questdb.kafka.domain.Order;
import io.questdb.kafka.domain.OrderStatus;
import io.questdb.kafka.domain.SensorReading;
import io.questdb.kafka.domain.Student;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import static java.time.Duration.ofMinutes;

/**
 * End-to-end test of the connector with Protobuf payloads and the Confluent Schema Registry.
 * Producers serialize with {@link KafkaProtobufSerializer}, the connector deserializes with
 * {@code io.confluent.connect.protobuf.ProtobufConverter} shipped in the cp-kafka-connect image.
 */
@Testcontainers
public class ProtobufSchemaRegistryIT {
    // we need to locate JARs with QuestDB client and Kafka Connect Connector,
    // this is later used to copy to the Kafka Connect container
    @RegisterExtension
    public static JarResolverExtension connectorJarResolver = JarResolverExtension.forClass(QuestDBSinkTask.class);
    @RegisterExtension
    public static JarResolverExtension questdbJarResolver = JarResolverExtension.forClass(Sender.class);

    private final static Network network = Network.newNetwork();

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withKraft()
            .withEnv("KAFKA_BROKER_ID", "0")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "0@kafka:9094");

    @Container
    private final GenericContainer<?> questDBContainer = new GenericContainer<>("questdb/questdb:10.0.0")
            .withNetwork(network)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @Container
    private final DebeziumContainer connectContainer = new DebeziumContainer("confluentinc/cp-kafka-connect:7.8.0")
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
    private GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
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
                    .setBirthday(timestamp("2000-01-01T00:00:00Z"))
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
                    .setBirthday(timestamp("2000-01-01T00:00:00Z"))
                    .build();
            producer.send(new ProducerRecord<>(topicName, "foo", student)).get();
        }
        startConnector(topicName, "birthday");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2000-01-01T00:00:00.000000Z\"\r\n",
                "select * from " + topicName, questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));

        // a newer version of the schema, with an extra field, registered under the same subject.
        // the schema is parsed at runtime, so no generated classes are involved.
        try (Producer<String, Message> producer = new KafkaProducer<>(producerProps())) {
            ProtobufSchema schema = new ProtobufSchema(readResource("/proto-runtime/student_with_extra_column.proto"));
            Descriptors.Descriptor descriptor = schema.toDescriptor("Student");
            DynamicMessage.Builder student = DynamicMessage.newBuilder(descriptor);
            JsonFormat.parser().merge("{\"firstname\":\"Mary\",\"lastname\":\"Doe\",\"birthday\":\"2005-01-01T00:00:00Z\",\"active\":true}", student);
            producer.send(new ProducerRecord<>(topicName, "foo", student.build())).get();
        }
        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\",\"active\"\r\n"
                        + "\"John\",\"Doe\",\"2000-01-01T00:00:00.000000Z\",false\r\n"
                        + "\"Mary\",\"Doe\",\"2005-01-01T00:00:00.000000Z\",true\r\n",
                "select * from " + topicName, questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    @Test
    public void testProtobufRecordsWithArrays() throws Exception {
        String topicName = "sensors";

        // sensor reading with a repeated double field
        try (Producer<String, SensorReading> producer = new KafkaProducer<>(producerProps())) {
            SensorReading reading = SensorReading.newBuilder()
                    .setSensorId("sensor-001")
                    .setTimestamp(timestamp("2024-01-01T10:00:00Z"))
                    .addAllValues(Arrays.asList(22.5, 23.1, 22.8, 23.3, 22.9))
                    .setLocation("Building A")
                    .build();
            producer.send(new ProducerRecord<>(topicName, "key1", reading)).get();

            // location is not set: proto3 "optional" fields without a value arrive as null
            SensorReading reading2 = SensorReading.newBuilder()
                    .setSensorId("sensor-002")
                    .setTimestamp(timestamp("2024-01-01T10:05:00Z"))
                    .addAllValues(Arrays.asList(18.2, 18.5, 18.3))
                    .build();
            producer.send(new ProducerRecord<>(topicName, "key2", reading2)).get();
        }

        startConnector(topicName, "timestamp");

        QuestDBUtils.assertSqlEventually(
                "\"sensor_id\",\"values\",\"location\",\"timestamp\"\r\n" +
                        "\"sensor-001\",\"[22.5,23.1,22.8,23.3,22.9]\",\"Building A\",\"2024-01-01T10:00:00.000000Z\"\r\n" +
                        "\"sensor-002\",\"[18.2,18.5,18.3]\",,\"2024-01-01T10:05:00.000000Z\"\r\n",
                "select sensor_id, \"values\", location, timestamp from " + topicName + " order by timestamp",
                questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    @Test
    public void testNestedMessagesAndEnums() throws Exception {
        String topicName = "orders";

        try (Producer<String, Order> producer = new KafkaProducer<>(producerProps())) {
            Order order = Order.newBuilder()
                    .setOrderId("order-1")
                    .setCreatedAt(timestamp("2024-03-01T12:00:00Z"))
                    .setStatus(OrderStatus.SHIPPED)
                    .setShipping(Address.newBuilder().setCity("London").setCountry("UK"))
                    .setQuantity(3)
                    .setPrice(19.99)
                    .build();
            producer.send(new ProducerRecord<>(topicName, "order-1", order)).get();

            // nested message not set: its columns are left empty
            Order order2 = Order.newBuilder()
                    .setOrderId("order-2")
                    .setCreatedAt(timestamp("2024-03-01T12:05:00Z"))
                    .setStatus(OrderStatus.PLACED)
                    .setQuantity(1)
                    .setPrice(5.0)
                    .build();
            producer.send(new ProducerRecord<>(topicName, "order-2", order2)).get();
        }

        startConnector(topicName, "created_at");

        // enums arrive as their symbolic name, nested messages are flattened with an underscore
        QuestDBUtils.assertSqlEventually(
                "\"order_id\",\"status\",\"shipping_city\",\"shipping_country\",\"quantity\",\"price\",\"timestamp\"\r\n" +
                        "\"order-1\",\"SHIPPED\",\"London\",\"UK\",3,19.99,\"2024-03-01T12:00:00.000000Z\"\r\n" +
                        "\"order-2\",\"PLACED\",,,1,5.0,\"2024-03-01T12:05:00.000000Z\"\r\n",
                "select * from " + topicName + " order by timestamp",
                questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }

    private void startConnector(String topicName, String timestampName) {
        // QWP transport (ws::), needs QuestDB 10 or newer
        String confString = "ws::addr=" + questDBContainer.getNetworkAliases().get(0) + ":" + QuestDBUtils.QUESTDB_HTTP_PORT + ";auto_flush_rows=1;";
        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", QuestDBSinkConnector.class.getName())
                .with("tasks.max", "1")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.protobuf.ProtobufConverter")
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
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());
        return props;
    }

    private static Timestamp timestamp(String iso) {
        return Timestamps.fromMillis(Instant.parse(iso).toEpochMilli());
    }

    private String readResource(String name) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("resource not found: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}

package io.questdb.kafka.samples;

import com.google.protobuf.util.Timestamps;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random trades, serializes them as Protobuf with the Confluent serializer
 * and sends them to a Kafka topic. The serializer registers the schema of {@link Trade}
 * in the Schema Registry on the first send.
 */
public final class TradesProducer {
    private static final Logger log = LoggerFactory.getLogger(TradesProducer.class);

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA"};
    private static final String[] EXCHANGES = {"NASDAQ", "NYSE"};

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        String topic = env("TOPIC", "trades");
        long delayMillis = Long.parseLong(env("DELAY_MS", "500"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        double[] prices = new double[SYMBOLS.length];
        for (int i = 0; i < prices.length; i++) {
            prices[i] = 100 + ThreadLocalRandom.current().nextDouble(400);
        }

        log.info("Producing trades to topic '{}' via {} (schema registry: {})", topic, bootstrapServers, schemaRegistryUrl);
        long sent = 0;
        try (Producer<String, Trade> producer = new KafkaProducer<>(props)) {
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                int i = rnd.nextInt(SYMBOLS.length);
                // random walk, so the price chart looks plausible
                prices[i] = Math.max(1, prices[i] * (1 + rnd.nextDouble(-0.005, 0.005)));

                Trade trade = Trade.newBuilder()
                        .setSymbol(SYMBOLS[i])
                        .setSide(rnd.nextBoolean() ? Side.BUY : Side.SELL)
                        .setPrice(Math.round(prices[i] * 100) / 100.0)
                        .setQuantity(rnd.nextLong(1, 1_000))
                        .setExchange(EXCHANGES[rnd.nextInt(EXCHANGES.length)])
                        .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();

                try {
                    // block on the send: Kafka and the Schema Registry may still be starting,
                    // in which case we log the error and simply try again
                    producer.send(new ProducerRecord<>(topic, trade.getSymbol(), trade)).get();
                    if (++sent % 100 == 0) {
                        log.info("Sent {} trades, last one: {}", sent, trade.toString().replace('\n', ' '));
                    }
                } catch (Exception e) {
                    log.warn("Failed to send a trade, retrying: {}", e.getMessage());
                }
                Thread.sleep(delayMillis);
            }
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }
}

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

    private static final String[] SYMBOLS = {"BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT"};
    private static final double[] START_PRICES = {76853.5, 4102.2, 100.45, 2.71, 0.2134};

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        String topic = env("TOPIC", "trades");
        long delayMillis = Long.parseLong(env("DELAY_MS", "20"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        double[] prices = START_PRICES.clone();

        log.info("Producing trades to topic '{}' via {} (schema registry: {})", topic, bootstrapServers, schemaRegistryUrl);
        long sent = 0;
        try (Producer<String, Trade> producer = new KafkaProducer<>(props)) {
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                int i = rnd.nextInt(SYMBOLS.length);
                // random walk of at most 0.5% per trade, so a price chart looks plausible
                prices[i] = Math.max(0.0001, prices[i] * (1 + rnd.nextDouble(-0.005, 0.005)));

                Trade trade = Trade.newBuilder()
                        .setSymbol(SYMBOLS[i])
                        .setSide(rnd.nextBoolean() ? "buy" : "sell")
                        .setPrice(round(prices[i], 8))
                        .setAmount(round(rnd.nextDouble(0.0001, 2.0), 6))
                        .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();

                try {
                    // block on the send: Kafka and the Schema Registry may still be starting,
                    // in which case we log the error and simply try again.
                    // The message key is the symbol, so all trades of one symbol stay in order.
                    producer.send(new ProducerRecord<>(topic, trade.getSymbol(), trade)).get();
                    if (++sent % 500 == 0) {
                        log.info("Sent {} trades, last one: {}", sent, trade.toString().replace('\n', ' '));
                    }
                } catch (Exception e) {
                    log.warn("Failed to send a trade, retrying: {}", e.getMessage());
                }
                Thread.sleep(delayMillis);
            }
        }
    }

    private static double round(double value, int decimals) {
        double scale = Math.pow(10, decimals);
        return Math.round(value * scale) / scale;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }
}

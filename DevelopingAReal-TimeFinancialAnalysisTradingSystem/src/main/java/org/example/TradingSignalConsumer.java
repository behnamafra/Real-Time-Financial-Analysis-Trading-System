package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TradingSignalConsumer {
    public static void main(String[] args) {
        startTradingSignalConsumer();
    }

    public static void startTradingSignalConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "trading-signal-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("financial-data"));
            System.out.println("Data received in consumer successfully");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // Process and handle trading signals for each record
                records.forEach(record -> {
                    String tradingSignal = record.value();
                    // Implement your logic to handle trading signals
                    handleTradingSignal(tradingSignal);
                });
            }
        }
    }

    private static void handleTradingSignal(String tradingSignal) {
        // Implement your logic to handle trading signals
        System.out.println("Received trading signal: " + tradingSignal);
    }
}

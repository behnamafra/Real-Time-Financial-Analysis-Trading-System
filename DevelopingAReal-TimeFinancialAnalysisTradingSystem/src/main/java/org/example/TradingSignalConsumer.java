package org.example;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


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
                    analyzeTradingSignal(tradingSignal);
                    System.out.println("analyzed-data sent to python consumer successfully");
                });
            }
        }
    }
    private static void analyzeTradingSignal(String tradingSignal) {
        Properties propsw = new Properties();
        propsw.put("bootstrap.servers", "localhost:9092");
        propsw.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsw.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(propsw);
        JsonNode signalNode = parseJson(tradingSignal);
        String stockSymbol = signalNode.has("stock_symbol") ? signalNode.get("stock_symbol").asText() : null;
        boolean isBuySignal = isBuySignal(tradingSignal);
        boolean isSellSignal = isSellSignal(tradingSignal);
        System.out.println("BuySignal" + isBuySignal);
        if (isBuySignal) {
            producer.send(new ProducerRecord<>("analyzed-data", "Buy"));
        } else if (isSellSignal) {
            System.out.println("Sell Signal for " + stockSymbol + ": Consider selling shares based on the analysis.");
            producer.send(new ProducerRecord<>("analyzed-data", "Sell"));
        } else {
            System.out.println("No clear buy or sell signal for " + stockSymbol + " based on the analysis.");
            producer.send(new ProducerRecord<>("analyzed-data", "No action"));
        }
    }
    private static boolean isBuySignal(String tradingSignal) {
        JsonNode signalNode = parseJson(tradingSignal);

        if (signalNode == null) {
            // Handle parsing error
            return false;
        }

        // Moving Averages: Buy when short-term MA crosses above long-term MA
        boolean maBuySignal = hasDoubleValue(signalNode, "opening_price") &&
                hasDoubleValue(signalNode, "closing_price") &&
                signalNode.get("opening_price").asDouble() > signalNode.get("closing_price").asDouble();

        // RSI: Buy when RSI is below a certain threshold (e.g., 30)
        boolean rsiBuySignal = hasDoubleValue(signalNode, "rsi") &&
                signalNode.get("rsi").asDouble() < 30;

        // Price Trend: Buy in an upward trend
//        boolean upwardTrend = hasDoubleValue(signalNode, "closing_price") &&
//                hasDoubleValue(signalNode, "opening_price") &&
//                signalNode.get("closing_price").asDouble() > signalNode.get("opening_price").asDouble();

        // Volume Analysis: Buy on high trading volume during an upward price movement
//        boolean highVolumeBuySignal = hasIntValue(signalNode, "volume") &&
//                signalNode.get("volume").asInt() > 1000; // Adjust the threshold as needed

        // Support and Resistance Levels: Buy on a bounce off a support level
//        boolean supportBounceBuySignal = hasDoubleValue(signalNode, "low") &&
//                hasDoubleValue(signalNode, "opening_price") &&
//                signalNode.get("low").asDouble() > signalNode.get("opening_price").asDouble();

        // Combine buy signals (you might need a more sophisticated strategy)
        //if (maBuySignal || rsiBuySignal || upwardTrend || highVolumeBuySignal || supportBounceBuySignal)
        //return
//        int trueConditions = 0;
//        System.out.println("maBuySignal" + maBuySignal);
//        System.out.println("rsiBuySignal" + rsiBuySignal);

        return maBuySignal && rsiBuySignal;
    }

    private static boolean isSellSignal(String tradingSignal) {
        JsonNode signalNode = parseJson(tradingSignal);

        if (signalNode == null) {
            // Handle parsing error
            return false;
        }

        // Moving Averages: Sell when short-term MA crosses below long-term MA
        boolean maSellSignal = hasDoubleValue(signalNode, "opening_price") &&
                hasDoubleValue(signalNode, "closing_price") &&
                signalNode.get("opening_price").asDouble() < signalNode.get("closing_price").asDouble();

        // RSI: Sell when RSI is above a certain threshold (e.g., 70)
        boolean rsiSellSignal = hasDoubleValue(signalNode, "rsi") &&
                signalNode.get("rsi").asDouble() > 70;

        // Price Trend: Sell in a downward trend
//        boolean downwardTrend = hasDoubleValue(signalNode, "closing_price") &&
//                hasDoubleValue(signalNode, "opening_price") &&
//                signalNode.get("closing_price").asDouble() < signalNode.get("opening_price").asDouble();

        // Volume Analysis: Sell on high trading volume during a downward price movement
//        boolean highVolumeSellSignal = hasIntValue(signalNode, "volume") &&
//                signalNode.get("volume").asInt() > 1000; // Adjust the threshold as needed

        // Support and Resistance Levels: Sell on rejection at a resistance level
//        boolean resistanceRejectSellSignal = hasDoubleValue(signalNode, "high") &&
//                hasDoubleValue(signalNode, "closing_price") &&
//                signalNode.get("high").asDouble() < signalNode.get("closing_price").asDouble();

        // Combine sell signals (you might need a more sophisticated strategy)
        return maSellSignal && rsiSellSignal;
    }

    private static boolean hasDoubleValue(JsonNode node, String key) {
        return node.has(key) && node.get(key).isNumber();
    }

    private static boolean hasIntValue(JsonNode node, String key) {
        return node.has(key) && node.get(key).isInt();
    }

    private static JsonNode parseJson(String jsonString) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            // Handle parsing exception
            e.printStackTrace();
            return null;
        }
        }


}




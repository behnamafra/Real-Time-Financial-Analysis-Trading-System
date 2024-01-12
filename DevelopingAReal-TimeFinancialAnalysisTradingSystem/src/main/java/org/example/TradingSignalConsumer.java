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
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class TradingSignalConsumer {
    private static final int MOVING_AVERAGE_WINDOW = 5;
    private static final Queue<Double> closingPricesQueue = new LinkedList<>();

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
        double closingPrice = signalNode.has("closing_price") ? signalNode.get("closing_price").asDouble() : 0.0;

        // Update the closing prices queue
        closingPricesQueue.offer(closingPrice);
        if (closingPricesQueue.size() > MOVING_AVERAGE_WINDOW) {
            closingPricesQueue.poll(); // Remove the oldest element if the window is full
        }

        double movingAverage = calculateMovingAverage(closingPricesQueue);

        boolean isBuySignal = isBuySignal(movingAverage, closingPrice);
        boolean isSellSignal = isSellSignal(movingAverage, closingPrice);

        System.out.println("Moving Average: " + movingAverage);
        System.out.println("BuySignal: " + isBuySignal);

        if (isBuySignal) {
            System.out.println("Buy Signal for " + stockSymbol + ": Consider selling shares based on the analysis.");
            producer.send(new ProducerRecord<>("analyzed-data", "Buy"));
        } else if (isSellSignal) {
            System.out.println("Sell Signal for " + stockSymbol + ": Consider selling shares based on the analysis.");
            producer.send(new ProducerRecord<>("analyzed-data", "Sell"));
        } else {
            System.out.println("No clear buy or sell signal for " + stockSymbol + " based on the analysis.");
            producer.send(new ProducerRecord<>("analyzed-data", "No action"));
        }
    }
    private static double calculateMovingAverage(Queue<Double> pricesQueue) {
        double sum = 0.0;
        for (Double price : pricesQueue) {
            sum += price;
        }
        return sum / pricesQueue.size();
    }
    private static boolean isBuySignal(double movingAverage, double currentClosingPrice) {
        // Add your buy signal logic here based on the Moving Average and current closing price
        // For example, you can check if the current closing price is above the Moving Average.
        return currentClosingPrice > movingAverage;
    }

    private static boolean isSellSignal(double movingAverage, double currentClosingPrice) {
        // Add your sell signal logic here based on the Moving Average and current closing price
        // For example, you can check if the current closing price is below the Moving Average.
        return currentClosingPrice < movingAverage;
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




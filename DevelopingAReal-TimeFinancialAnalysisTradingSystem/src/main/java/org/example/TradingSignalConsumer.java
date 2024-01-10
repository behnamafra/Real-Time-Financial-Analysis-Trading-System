package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

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
                    //System.out.println("Record :" + tradingSignal );
                    analyzeTradingSignal(tradingSignal);
                });
            }
        }
    }

    private static void analyzeTradingSignal(String tradingSignal) {
        JsonNode signalNode = parseJson(tradingSignal);
        String stockSymbol = signalNode.has("stock_symbol") ? signalNode.get("stock_symbol").asText() : null;
        boolean isBuySignal = isBuySignal(tradingSignal);
        boolean isSellSignal = isSellSignal(tradingSignal);
        System.out.println("BuySignal" + isBuySignal);
        if (isBuySignal) {
            System.out.println("Buy Signal for " + stockSymbol + ": Consider buying shares based on the analysis.");
        } else if (isSellSignal) {
            System.out.println("Sell Signal for " + stockSymbol + ": Consider selling shares based on the analysis.");
        } else {
            System.out.println("No clear buy or sell signal for " + stockSymbol + " based on the analysis.");
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
        boolean upwardTrend = hasDoubleValue(signalNode, "closing_price") &&
                hasDoubleValue(signalNode, "opening_price") &&
                signalNode.get("closing_price").asDouble() > signalNode.get("opening_price").asDouble();

        // Volume Analysis: Buy on high trading volume during an upward price movement
        boolean highVolumeBuySignal = hasIntValue(signalNode, "volume") &&
                signalNode.get("volume").asInt() > 1000; // Adjust the threshold as needed

        // Support and Resistance Levels: Buy on a bounce off a support level
        boolean supportBounceBuySignal = hasDoubleValue(signalNode, "low") &&
                hasDoubleValue(signalNode, "opening_price") &&
                signalNode.get("low").asDouble() > signalNode.get("opening_price").asDouble();

        // Combine buy signals (you might need a more sophisticated strategy)
        //if (maBuySignal || rsiBuySignal || upwardTrend || highVolumeBuySignal || supportBounceBuySignal)
        //return
        int trueConditions = 0;
        System.out.println("maBuySignal" + maBuySignal);
        System.out.println("rsiBuySignal" + rsiBuySignal);
        System.out.println("upwardTrend" + upwardTrend);
        System.out.println("highVolumeBuySignal" + highVolumeBuySignal);
        System.out.println("supportBounceBuySignal" + supportBounceBuySignal);

        if (maBuySignal) trueConditions++;
        if (rsiBuySignal) trueConditions++;
        if (upwardTrend) trueConditions++;
        if (highVolumeBuySignal) trueConditions++;
        if (supportBounceBuySignal) trueConditions++;
        System.out.println("trueConditions" + trueConditions);

        if (trueConditions >=3) ;
    }
    // At least three of the conditions are true
    // Your code here
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
        boolean downwardTrend = hasDoubleValue(signalNode, "closing_price") &&
                hasDoubleValue(signalNode, "opening_price") &&
                signalNode.get("closing_price").asDouble() < signalNode.get("opening_price").asDouble();

        // Volume Analysis: Sell on high trading volume during a downward price movement
        boolean highVolumeSellSignal = hasIntValue(signalNode, "volume") &&
                signalNode.get("volume").asInt() > 1000; // Adjust the threshold as needed

        // Support and Resistance Levels: Sell on rejection at a resistance level
        boolean resistanceRejectSellSignal = hasDoubleValue(signalNode, "high") &&
                hasDoubleValue(signalNode, "closing_price") &&
                signalNode.get("high").asDouble() < signalNode.get("closing_price").asDouble();

        // Combine sell signals (you might need a more sophisticated strategy)
        return maSellSignal || rsiSellSignal || downwardTrend || highVolumeSellSignal || resistanceRejectSellSignal;
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


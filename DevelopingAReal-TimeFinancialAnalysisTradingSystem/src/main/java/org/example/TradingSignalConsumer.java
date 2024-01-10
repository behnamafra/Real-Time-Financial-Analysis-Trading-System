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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

public class TradingSignalConsumer {
    private static   NotificationService notificationService = null;


    @Autowired
    public TradingSignalConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
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
        //System.out.println("BuySignal" + isBuySignal);
        if (isBuySignal) {
            System.out.println("Buy Signal for " + stockSymbol + ": Consider buying shares based on the analysis.");
            TradingSignalConsumer consumer = new TradingSignalConsumer(notificationService);
            consumer.sendNotification("Buy Signal for " + stockSymbol);

        } else if (isSellSignal) {
            System.out.println("Sell Signal for " + stockSymbol + ": Consider selling shares based on the analysis.");
            TradingSignalConsumer consumer = new TradingSignalConsumer(notificationService);
            consumer.sendNotification("Buy Signal for " + stockSymbol);
        } else {
            System.out.println("No clear buy or sell signal for " + stockSymbol + " based on the analysis.");
        }
    }
    public  void sendNotification(String message) {
        if (notificationService != null) {
            notificationService.sendNotification(message);
        } else {
            // Handle the case where notificationService is null
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

        // Exponential Moving Average (EMA): Buy when short-term EMA crosses above long-term EMA
        int shortTermEMAPeriod = 10; // Adjust as needed
        int longTermEMAPeriod = 50;  // Adjust as needed
        double shortTermEMA = calculateEMA(signalNode, "closing_price", shortTermEMAPeriod);
        double longTermEMA = calculateEMA(signalNode, "closing_price", longTermEMAPeriod);
        boolean emaBuySignal = shortTermEMA > longTermEMA;

        // Combine buy signals (you might need a more sophisticated strategy)
        //if (maBuySignal || rsiBuySignal || upwardTrend || highVolumeBuySignal || supportBounceBuySignal)
        //return
        int trueConditions = 0;
//        System.out.println("maBuySignal" + maBuySignal);
//        System.out.println("rsiBuySignal" + rsiBuySignal);
//        System.out.println("upwardTrend" + upwardTrend);
//        System.out.println("highVolumeBuySignal" + highVolumeBuySignal);
//        System.out.println("supportBounceBuySignal" + supportBounceBuySignal);

        if (maBuySignal) trueConditions++;
        if (emaBuySignal) trueConditions++;
        if (rsiBuySignal) trueConditions++;
        if (upwardTrend) trueConditions++;
        if (highVolumeBuySignal) trueConditions++;
        if (supportBounceBuySignal) trueConditions++;
        System.out.println("trueConditions Buy" + trueConditions);
       // System.out.println("emaBuySignal" + emaBuySignal);

        return trueConditions >= 3;
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

        // EMI: Sell when Earnings per Share is above a certain threshold
        boolean emiSellSignal = hasDoubleValue(signalNode, "emi") &&
                signalNode.get("emi").asDouble() > 0.5; // Adjust the threshold as needed


        // Combine sell signals (you might need a more sophisticated strategy)
        //return maSellSignal || rsiSellSignal || downwardTrend || highVolumeSellSignal || resistanceRejectSellSignal;
        int trueConditions = 0;
        if (maSellSignal) trueConditions++;
        if (emiSellSignal) trueConditions++;
        if (rsiSellSignal) trueConditions++;
        if (downwardTrend) trueConditions++;
        if (highVolumeSellSignal) trueConditions++;
        if (resistanceRejectSellSignal) trueConditions++;
        System.out.println("trueCondition Sell" + trueConditions);


        return trueConditions >= 3;
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
    // At least three of the conditions are true
    // Your code here
    private static double calculateEMA(JsonNode signalNode, String key, int period) {
        if (!hasDoubleValue(signalNode, key)) {
            return 0.0; // Handle the case where the required field is missing
        }

        double multiplier = 2.0 / (period + 1);
        double currentValue = signalNode.get(key).asDouble();
        double previousEMA = calculatePreviousEMA(signalNode, key, period);
        return (currentValue - previousEMA) * multiplier + previousEMA;
    }

    private static double calculatePreviousEMA(JsonNode signalNode, String key, int period) {
        // For simplicity, let's assume you have a variable to store the previous EMA
        // You may need to fetch the previous EMA from a data structure or database
        // For now, let's use a simple variable as an example
        double previousEMA = 0.0; // Default value
        // You may use a data structure or store it in a variable
        // For simplicity, let's assume a default value of 0.0
        return previousEMA;
    }

}


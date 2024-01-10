package org.example;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class FinancialDataValidator {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static boolean isValidFinancialData(String data) {
        try {
            JsonNode jsonNode = objectMapper.readTree(data);

            // Check if the data has the required fields based on its type
            if (jsonNode.has("stock_symbol")) {
                return validateMarketData(jsonNode);
            } else if (jsonNode.has("data_type")) {
                return validateOtherData(jsonNode);
            } else {
                return false; // Unknown data type
            }
        } catch (Exception e) {
            // An exception occurred, indicating invalid JSON format
            return false;
        }
    }

    private static boolean validateMarketData(JsonNode jsonNode) {
        // Validate fields for market_data
        return jsonNode.has("opening_price") &&
                jsonNode.has("closing_price") &&
                jsonNode.has("high") &&
                jsonNode.has("low") &&
                jsonNode.has("volume") &&
                jsonNode.has("timestamp");
    }

    private static boolean validateOtherData(JsonNode jsonNode) {
        // Validate fields for other data types (e.g., market_data, economic_indicator, order_book, news_sentiment)
        return jsonNode.has("timestamp") &&
                jsonNode.has("stock_symbol");
        // You can add more specific validations based on the data type
    }
}

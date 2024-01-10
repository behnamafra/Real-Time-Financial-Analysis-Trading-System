package org.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class DataHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        InputStream is = exchange.getRequestBody();
        StringBuilder requestBody = new StringBuilder();
        int i;
        while ((i = is.read()) != -1) {
            requestBody.append((char) i);
        }
        is.close();

        // Validate the received data
        if (FinancialDataValidator.isValidFinancialData(requestBody.toString())) {
            // If the data is valid, send it to Kafka for further processing
            sendToKafka(requestBody.toString());
            // Send a response back to the Python script
            System.out.println("Valid data received: " + requestBody);
            System.out.println("Data received and sent to Kafka successfully");
            String response = "Data received and sent to Kafka successfully";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        } else {
            // Handle invalid data (you can customize this part)
            //System.out.println("Invalid data received: " + requestBody);

            // Send an error response back to the Python script
            String response = "Invalid data format";
            exchange.sendResponseHeaders(400, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    private void sendToKafka(String data) {
        // Implement your logic to send data to Kafka
        // This could involve calling the FinancialDataProducer class
        // For simplicity, we'll assume a method `sendToKafka` in FinancialDataProducer
        //TradingSignalConsumer.startTradingSignalConsumer();
        FinancialDataProducer.sendFinancialDataToKafka(data);

    }


}

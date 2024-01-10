package org.example;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DataServer {

    public static void main(String[] args) throws IOException {
        int serverPort = 8080;
        HttpServer server = HttpServer.create(new InetSocketAddress(serverPort), 0);
        server.createContext("/ingest", new DataHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("Server started on port " + serverPort);

        // Run the Python script as a subprocess
        runPythonScript();
    }
    private static void runPythonScript() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("python", "DevelopingAReal-TimeFinancialAnalysisTradingSystem/generator.py");
            Process process = processBuilder.start();
            process.waitFor(); // Wait for the Python script to complete
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}


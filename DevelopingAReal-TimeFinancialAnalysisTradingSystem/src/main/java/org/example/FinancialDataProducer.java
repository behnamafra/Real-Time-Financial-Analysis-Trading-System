package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class FinancialDataProducer {
    public static void main(String[] args) {
        //sendFinancialDataToKafka();
    }
    public static void sendFinancialDataToKafka(String data) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        System.out.println("Data received from producer successfully"+data);
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("financial-data", data));
            System.out.println("Data sent to consumer successfully");
        }
    }

}

package com.goldenage.kafkademo;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer  {


    private final Producer<String, String> producer;

    public SimpleProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Отправляет сообщение в указанный топик.
     */
    public void sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("[PRODUCER] Отправлено: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), key, value);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }
}
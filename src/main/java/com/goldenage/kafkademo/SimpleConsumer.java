package com.goldenage.kafkademo;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Properties;
import java.time.format.DateTimeFormatter;
public class SimpleConsumer  implements Runnable {

    private final Consumer<String, String> consumer;
    private volatile boolean running = true;

    public SimpleConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "scheduled-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList("scheduled-topic"));
        System.out.println("[CONSUMER] Подписался на топик scheduled-topic, ожидаю сообщения...");

        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String receivedTime = LocalTime.now().format(timeFormatter);
                System.out.printf("[CONSUMER] %s | Получено: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                        receivedTime, record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
        consumer.close();
        System.out.println("[CONSUMER] Остановлен");
    }

    public void stop() {
        running = false;
    }
}

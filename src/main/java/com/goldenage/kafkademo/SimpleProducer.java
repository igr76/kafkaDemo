package com.goldenage.kafkademo;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class SimpleProducer  {

    public static void main(String[] args) {
        // Настройки Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // Дополнительные настройки
        props.put("acks", "all"); // гарантия доставки
        props.put("retries", 3); // количество повторов

        // Создание Producer
        Producer<String, String> producer =
                new KafkaProducer<>(props);

        try {
            // Отправка сообщения
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("my-topic", "key1", "Hello Kafka!");

            // Асинхронная отправка с callback
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println("Отправлено успешно: " +
                                "topic=" + metadata.topic() +
                                ", partition=" + metadata.partition() +
                                ", offset=" + metadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });

            // Синхронная отправка
            // RecordMetadata metadata = producer.send(record).get();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

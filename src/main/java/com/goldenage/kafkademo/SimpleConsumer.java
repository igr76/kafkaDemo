package com.goldenage.kafkademo;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
public class SimpleConsumer  {
    public static void main(String[] args) {
        // Настройки Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "my-consumer-group"); // обязательный параметр

        // Настройки поведения
        props.put("enable.auto.commit", "true"); // авто-коммит оффсетов
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest"); // earliest/latest/none

        // Создание Consumer
        Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Подписка на топик
        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                // Опрос новых сообщений (таймаут 100 мс)
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Получено сообщение: topic=%s, partition=%d, offset=%d, " +
                                    "key=%s, value=%s%n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                }

                // Ручной коммит оффсетов (если enable.auto.commit=false)
                // consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}

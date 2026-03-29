package com.goldenage.kafkademo;
import java.time.format.DateTimeFormatter;
import java.time.LocalTime;
import java.util.Scanner;

public class KafkaScheduledDemo {
    public static void main(String[] args) throws InterruptedException {
        // Запускаем Consumer в отдельном потоке
        SimpleConsumer consumer = new SimpleConsumer();
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Даём время на подписку
        Thread.sleep(2000);

        // Создаём Producer
        SimpleProducer producer = new SimpleProducer();

        // Для красивого вывода времени
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        // Обработка остановки по нажатию Enter
        System.out.println("Программа будет отправлять сообщения каждые 5 секунд.");
        System.out.println("Нажмите Enter для остановки...");

        // Запускаем отдельный поток для ожидания ввода
        Thread inputThread = new Thread(() -> {
            new Scanner(System.in).nextLine();
            consumer.stop();
            producer.close();
            System.out.println("Остановка по запросу пользователя...");
        });
        inputThread.setDaemon(true);
        inputThread.start();

        // Основной цикл отправки
        int messageCount = 0;
        while (!Thread.currentThread().isInterrupted() && consumerThread.isAlive()) {
            String currentTime = LocalTime.now().format(timeFormatter);
            String message = "Сообщение #" + (++messageCount) + " в " + currentTime;
            producer.sendMessage("scheduled-topic", "key-" + messageCount, message);

            // Ждём 5 секунд перед следующей отправкой
            Thread.sleep(5000);
        }

        // Ждём завершения consumer
        consumerThread.join();
        System.out.println("Программа завершена.");
    }
}

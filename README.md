Ниже представлен полный пример, который демонстрирует отправку и получение сообщения с использованием Apache Kafka, запущенной в Docker. Всё, что вам потребуется – Docker и Java.

1. Запуск Kafka в Docker
   Создайте файл docker-compose.yml:    (здесь в документе вместо точек в начале строки подставить тире)
   или скопируйте из каталога этого репозитория         
yaml                                               
version: '3'                                       
services:                                
zookeeper:                          
image: confluentinc/cp-zookeeper:latest     
environment:                       
ZOOKEEPER_CLIENT_PORT: 2181                 
ZOOKEEPER_TICK_TIME: 2000               
ports:
- "2181:2181"

kafka:                          
image: confluentinc/cp-kafka:7.4.0 
depends_on:
- zookeeper                                   
  ports:
- "9092:9092"                       
  environment:                         
  KAFKA_BROKER_ID: 1                     
  KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

2. Для запуска файла надо войти в командную строку (CMD в поиске виндовс) перейти в каталог где находиться файл, и предварительно надо установить и запустить Docker Desctop ) и ввести команду docker-compose up -d  после завершения запуска можно проверить их работу командой docker-compose ps,  docker-compose down останавливает их работу
3. Запустить KafkaScheduledDemo

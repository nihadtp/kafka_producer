# Kafka Service App

Python application that concurrently sends out http request to NAV endpoint and writes the response to Kafka server.

## Pre-requisites

- Apache Kafka 2.13-2.8
- Java version "1.8.0_301"

## Steps

1. Clone the project
```
git https://github.com/nihadtp/kafka_producer.git
```

2. Got Kafka directory and open a terminal and start zookeeper and kafka server

```
sudo ./bin/zookeeper-server-start.sh config/zookeeper.properties
sudo ./bin/kafka-server-start.sh config/server.properties
```

3. Create an input topic. Use the same name in the spark streaming app configuration.

```
./kafka-topics.sh --create --topic mutualFundTopic --if-not-exists --partitions 4 --config max.message.bytes=10000000 --bootstrap-server localhost:9092
```

4. Create an error topic to send back invalid data

```
./kafka-topics.sh --create --topic error_topic --if-not-exists --partitions 1 --config max.message.bytes=10000000 --bootstrap-server localhost:9092
```

5. Go to main project directory- kafka_producer and instal dependencies

```
pip install -r requirements.txt
```

7. Run the python application

```
python kafka_service.py
```

Python application will start sending out http response to kafka. I have added logger to see the real time status.


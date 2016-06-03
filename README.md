# apache-kafka-tests

First attempts at using apache kafka (consumers-producers-streams)

To use you'll need:

-Linux (tested with Ubuntu 15.10 x64)
-Java (Oracle JDK 8 or OpenJDK8)

## Before launching


### Install Kafka:

	mkdir ~/kafka-0.10
	cd ~/kafka-0.10
	wget https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz
	tar -xzf kafka_2.11-0.10.0.0.tgz
	cd kafka_2.11-0.10.0.0
	
#### Launch zookeeper

	bin/zookeeper-server-start.sh config/zookeeper.properties
	
#### Launch kafka broker

By default kafka runs on the 9092 port, in the default configs (provided by the several "defaultConfig()" methods) the broker port is 9192, due to the fact that I'm running Kapacitor on my laptop and it just happens that it uses the same port.. :D

	bin/kafka-server-start.sh config/server.properties
	
#### Create topic

	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ticks
	
Some experiments uses topic "ticks2", just check the code beforehand for the right topic name (you can just change it in the code)

### Install Influxdb *

*you need to do this only if you intend to use the "influxdbtickoutput" class (or if you want to run the tests..)
Google it :D

#### Create database dbkafkatest
	
	influx
	CREATE DATABASE dbkafkatest
	
## Things that "kinda" works:

- Kafka producer
- Kafka consumer
- Basic Tick generator/simulator
- InfluxDB output
- Console output
- Tick SERDEs



# KStreams API for stateful processing
---

### Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.
The project has 2 main components:
- `StateStoreExample.java`
- `SlotDataProducer.java`

`StateStoreExample.java` is the stream processing app while `SlotDataProducer.java` produces mock data into the `slot-data` topic



### Prerequisites

What things you need to install the software and how to install them


- Eclipse - https://www.eclipse.org/
- Java 1.8 or above - https://openjdk.java.net/
- Apache Kafka dist 2.5.0 - https://kafka.apache.org/


### Installation
The project uses version 2.4.1 of the KStreams API (see pom.xml)
This is the only depedency that is needed to write production ready stream processing applications
```xml
<dependencies>
    <dependency>
		<groupId>org.apache.kafka</groupId>
		<artifactId>kafka-streams</artifactId>
		<version>2.4.1</version>
	</dependency>
</dependencies>
```

You must have the proper topic structures before you run this or any demo project.
The project uses 2 topics:
- `slot-data`: the producer sends the updated state of the tester to this topic; this is also known as an update stream
- `board-churn-event`: this topic is for the actual business related events

Navigate to your kafka installation directory and execute the following commands to create the topics:
>Note: this is for windows, if on linux use the .sh scripts instead of the .bat 
The commands are the same otherwise.

```sh
## creates the slot data topic
> bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic slot-data --create

## creates the board-churn-event topic
> bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic board-churn-event --create

## launch zookeeper
> bin\windows\zookeeper-server-start.bat config\zookeeper.properties

## launch kafka
> bin\windows\kafka-server-start.bat config\server.properties

## (optional) create console consumer to monitor the io for each topic
> bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic slot-data --from-beginning
> bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic board-churn-event --from-beginning
```

> In Eclipse IDE
```sh
## SlotDataProducer.java
Right-click -> Run As.. -> Java Application
```
You should now see data coming into the `slot-data` topic
```sh
## StateStoreExample.java
Right-click -> Run As.. -> Java Application
```

You should now see e `INIT`, `ADD`, and `REMOVE` events being output into `board-churn-event` topic












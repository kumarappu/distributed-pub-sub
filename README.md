# Introduction 

Distributed Pub Sub Application (POC) will test latency of highly concurrent publisher/consumer against distributed kafka brokers. 

In this POC , on publisher side, 10k tickers are concurrently spittiing out  mock market data at the rate of  4 updates/sec/ticker and publishing it on a kafka topic with 12 partitions.  On consumer side , there is a consumer group consisting of 12 consumers, each concurrently processing messages from one partition.
Avergae payload size is 100Bytes, protobuf is used for seriliazation/deseriliazation

Average latencey for this test has been found under 2ms.


Tech stacks used : Java 8, akka, protobuf, kafka

#  How to run 

- Before building the package, change application.type to either producer or consumer in application.properties 
- Review the kafka producer and consumer side properties in producer-config.properties and consumer-config.properties respectively 
- compile the code with command  'mvn package'
- run with java -jar distributed-pub-sub-poc.jar


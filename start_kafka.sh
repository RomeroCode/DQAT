#!/bin/bash

# Start ZooKeeper Server
nohup zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties > /dev/null 2>&1 &


sleep 5

# Start Kafka Server
nohup kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties > /dev/null 2>&1 &


sleep 5

# Create Sensor Readings topic
kafka-topics.sh --create --topic sensor_readings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "Kafka is runnning and ready for use"
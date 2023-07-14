#!/bin/bash

KAFKA=/opt/Kafka/kafka_2.11-2.3.0/

# Go to kafka folder
cd $KAFKA

# Run Zookeeper server
sudo bin/zookeeper-server-start.sh config/zookeeper.properties &
# Run Kafka server
sudo bin/kafka-server-start.sh config/server.properties &
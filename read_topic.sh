#!/bin/bash

# Read topic from command line argument
SERVER=$1
TOPIC=$2
# Path to Kafka
KAFKA=/opt/Kafka/kafka_2.11-2.3.0/
# Go to kafka folder
cd $KAFKA

# Read topic from Kafka
bin/kafka-console-consumer.sh --bootstrap-server $SERVER --topic $TOPIC
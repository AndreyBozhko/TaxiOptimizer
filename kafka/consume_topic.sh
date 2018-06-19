#!/bin/bash

TOPIC=$1
KAFKA_PATH=/usr/local/kafka/bin

kafka-console-consumer.sh --bootstrap-servers localhost:9092 --from-beginning --topic $TOPIC

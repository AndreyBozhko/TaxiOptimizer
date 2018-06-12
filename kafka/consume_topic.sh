#!/bin/bash

TOPIC=$1
KAFKA_PATH=/usr/local/kafka/bin

$KAFKA_PATH/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic $TOPIC

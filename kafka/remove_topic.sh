#!/bin/bash

TOPIC=$1
KAFKA_PATH=/usr/local/kafka/bin

kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC

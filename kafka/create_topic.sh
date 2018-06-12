#!/bin/bash

TOPIC=$1
NUM_PARTITIONS=1
REPL_FACTOR=2

KAFKA_PATH=/usr/local/kafka/bin

$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic $TOPIC --partitions $NUM_PARTITIONS --replication-factor $REPL_FACTOR

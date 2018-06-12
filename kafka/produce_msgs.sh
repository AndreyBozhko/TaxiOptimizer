#!/bin/bash

TOPIC=$1
NUM_PARTITIONS=1

IP_ADDR=localhost:9092
ID=1

python producer.py $TOPIC $IP_ADDR $ID $NUM_PARTITIONS

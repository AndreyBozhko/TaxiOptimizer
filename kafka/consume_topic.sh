#!/bin/bash

TOPIC=$1

kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic $TOPIC

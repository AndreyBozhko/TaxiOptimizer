#!/bin/bash

TOPIC=$1

kafka-topics.sh --describe --zookeeper localhost:2181 --topic $TOPIC

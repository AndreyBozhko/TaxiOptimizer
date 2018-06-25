#!/bin/bash

CLUSTER_NAME=kafka-cluster

peg fetch ${CLUSTER_NAME}

for tech in ssh aws environment ; do
  peg install ${CLUSTER_NAME} $tech
done

for tech in zookeeper kafka ; do
  peg install ${CLUSTER_NAME} $tech
  peg service ${CLUSTER_NAME} $tech start
done

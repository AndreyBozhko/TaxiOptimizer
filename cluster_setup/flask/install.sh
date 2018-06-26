#!/bin/bash

CLUSTER_NAME=flask-node

peg fetch ${CLUSTER_NAME}

for tech in ssh aws environment ; do
  peg install ${CLUSTER_NAME} $tech
done

#!/bin/bash

MASTERNODE=ec2-34-237-30-114.compute-1.amazonaws.com
SLAVENODE1=ec2-34-206-69-68.compute-1.amazonaws.com
SLAVENODE2=ec2-23-22-51-249.compute-1.amazonaws.com
SLAVENODE3=ec2-35-171-180-163.compute-1.amazonaws.com

for i in 1 2 3; do
  NODE=SLAVENODE$i
  echo ${!NODE}
  scp -r ~/TaxiPassengerFinder/ ${!NODE}:
  ssh ubuntu@${!NODE} '/usr/local/spark/sbin/start-slave.sh spark://ec2-34-237-30-114.compute-1.amazonaws.com:7077'

done

#!/bin/bash

output=""
dest=/home/ubuntu/.profile

clusters=kafka-cluster,spark-batch-cluster,spark-stream-cluster


clusters=`echo $clusters | sed s/","/" "/g`

for cluster in $clusters ; do

	peg sshcmd-node $cluster 1 "git clone https://github.com/AndreyBozhko/TaxiOptimizer"
	peg sshcmd-node $cluster 1 "wget -P ~/TaxiOptimizer/ https://jdbc.postgresql.org/download/postgresql-42.2.2.jar"
	peg sshcmd-node $cluster 1 "pip install -r TaxiOptimizer/requirements.txt"

done

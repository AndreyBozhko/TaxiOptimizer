#!/bin/bash

# clones TaxiOptimizer to the clusters, downloads necessary .jar files and installs required Python packages

output=""
dest=/home/ubuntu/.profile

clusters=kafka-cluster,spark-batch-cluster,spark-stream-cluster,flask-node


clusters=`echo $clusters | sed s/","/" "/g`

for cluster in $clusters ; do

  peg sshcmd-cluster $cluster "sudo apt-get install bc"

	peg sshcmd-node $cluster 1 "git clone https://github.com/AndreyBozhko/TaxiOptimizer"
	peg sshcmd-node $cluster 1 "wget -P ~/TaxiOptimizer/ https://jdbc.postgresql.org/download/postgresql-42.2.2.jar"
	peg sshcmd-node $cluster 1 "pip install -r TaxiOptimizer/requirements.txt"

done

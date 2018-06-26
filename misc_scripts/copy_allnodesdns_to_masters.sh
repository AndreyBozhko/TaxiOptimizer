#!/bin/bash

output=""
dest=/home/ubuntu/.profile

clusters=kafka-cluster,spark-batch-cluster,spark-stream-cluster,flask-node


clusters=`echo $clusters | sed s/","/" "/g`


for cluster in $clusters ; do

	i=0
	CLUSTER=`echo $cluster | tr '[:lower:]' '[:upper:]' | sed s/"-"/"_"/g`
	for dns in `peg describe $cluster | grep ec2 | sed s/".*DNS: "//g` ; do

		output="$output\nexport ${CLUSTER}_$i=$dns"
		i=`expr $i + 1`

	done

done


for cluster in $clusters ; do

	peg sshcmd-node $cluster 1 "echo $'$output' >> $dest"

done

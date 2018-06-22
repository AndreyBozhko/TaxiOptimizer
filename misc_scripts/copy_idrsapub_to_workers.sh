#!/bin/bash

line=`cat ~/.ssh/id_rsa.pub`

clusters=kafka-cluster,spark-batch-cluster,spark-stream-cluster


clusters=`echo $clusters | sed s/","/" "/g`

for cluster in $clusters ; do

	i=0

	for dns in `peg fetch $cluster | grep ec2 | sed s/".*DNS: "/"ubuntu@"/g` ; do

		if [ `expr $i` -gt 0 ] ; then

			ssh -i ~/.ssh/*.pem $dns "echo $line >> /home/ubuntu/.ssh/authorized_keys"

		fi

		i=`expr $i + 1`

	done

done

#!/bin/bash

line=`cat ~/.ssh/id_rsa.pub`

for cluster in kafka-cluster spark-batch-cluster spark-stream-cluster ; do

  i=0

  for dns in `peg fetch $cluster | grep ec2 | sed s/".*DNS: "/"ubuntu@"/g` ; do

    if [ `expr $i` -gt 0 ] ; then
      ssh -i ~/.ssh/*.pem $dns "echo $line >> /home/ubuntu/.ssh/authorized_keys"
    fi

    i=`expr $i + 1`

  done

done

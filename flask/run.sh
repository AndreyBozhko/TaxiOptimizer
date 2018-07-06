#!/bin/bash

PGPASSWORD=`ssh ubuntu@$SPARK_STREAM_CLUSTER_0 cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`
export PGPASSWORD

sudo -E nohup python run.py &

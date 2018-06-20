#!/bin/bash

. $PWD/load_configs.sh

spark-submit --master spark://$EC2_MASTER_DNS:7077 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
             --py-files $AUX_FILES \
             streaming/stream_data.py \
             $KAFKACONFIGFILE $SCHEMAFILE2 $PSQLCONFIGFILE

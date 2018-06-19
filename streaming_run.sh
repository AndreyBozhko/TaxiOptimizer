#!/bin/bash

KAFKACONFIGFILE=$PWD/config/kafka.ini
SCHEMAFILE=$PWD/config/schema_for_streaming.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

AUX_FILES=$PWD/helpers/helpers.py

EC2_MASTER_DNS=`grep ec2 $SPARK_HOME/conf/spark-env.sh | sed s/".*DNS.."//g | sed s/".$"//g`
PGPASSWORD=`cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`

export EC2_MASTER_DNS
export PGPASSWORD

cd streaming/

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
             --py-files $AUX_FILES \
             stream_data.py \
             $KAFKACONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE


unset EC2_MASTER_DNS
unset PGPASSWORD

cd ..

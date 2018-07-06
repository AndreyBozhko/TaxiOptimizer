#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.config
SCHEMAFILE1=$PWD/config/schema_for_raw_data.config
SCHEMAFILE2=$PWD/config/schema_for_streaming.config
STREAMCONFIGFILE=$PWD/config/stream.config
PSQLCONFIGFILE=$PWD/config/postgresql.config
KAFKACONFIGFILE=$PWD/config/kafka.config

AUX_FILES=$PWD/helpers/helpers.py

PGPASSWORD=`ssh ubuntu@$SPARK_STREAM_CLUSTER_0 cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`
export PGPASSWORD


case $1 in

  --batch)

    spark-submit --master spark://$SPARK_BATCH_CLUSTER_0:7077 \
                 --jars $PWD/postgresql-42.2.2.jar \
                 --py-files $AUX_FILES \
                 --driver-memory 4G \
                 --executor-memory 4G \
                 batch_processing/main_batch.py \
                 $S3CONFIGFILE $SCHEMAFILE1 $PSQLCONFIGFILE
    ;;

  --stream)

    spark-submit --master spark://$SPARK_STREAM_CLUSTER_0:7077 \
                 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
                 --jars $PWD/postgresql-42.2.2.jar \
                 --py-files $AUX_FILES \
                 --driver-memory 4G \
                 --executor-memory 4G \
                 streaming/main_stream.py \
                 $KAFKACONFIGFILE $SCHEMAFILE2 $STREAMCONFIGFILE $PSQLCONFIGFILE
    ;;

  *)

    echo "Usage: ./spark-run.sh [--batch|--stream]"
    ;;

esac

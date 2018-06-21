#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE1=$PWD/config/schema_for_raw_data.ini
SCHEMAFILE2=$PWD/config/schema_for_streaming.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

AUX_FILES=$PWD/helpers/helpers.py

EC2_MASTER_DNS=`grep ec2 $SPARK_HOME/conf/spark-env.sh | sed s/".*DNS.."//g | sed s/".$"//g`
PGPASSWORD=`cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`

export EC2_MASTER_DNS
export PGPASSWORD


case $1 in

  batch)

    spark-submit --master spark://$EC2_MASTER_DNS:7077 \
                 --jars $PWD/postgresql-42.2.2.jar \
                 --py-files $AUX_FILES \
                 --executor-memory 4G \
                 batch_processing/populate_database.py \
                 $S3CONFIGFILE $SCHEMAFILE1 $PSQLCONFIGFILE
    ;;

  stream)

    spark-submit --master spark://$EC2_MASTER_DNS:7077 \
                 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
                 --py-files $AUX_FILES \
                 streaming/stream_data.py \
                 $KAFKACONFIGFILE $SCHEMAFILE2 $PSQLCONFIGFILE
    ;;

  *)

    echo "Usage: ./run.sh [batch|stream]"
    ;;

esac

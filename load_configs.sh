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

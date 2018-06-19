#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/schema_for_raw_data.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

AUX_FILES=$PWD/helpers/helpers.py

EC2_MASTER_DNS=`grep ec2 $SPARK_HOME/conf/spark-env.sh | sed s/".*DNS.."//g | sed s/".$"//g`
PGPASSWORD=`cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`

export EC2_MASTER_DNS
export PGPASSWORD

echo "Spark Job executing..."

cd batch_processing/

spark-submit --master spark://$EC2_MASTER_DNS:7077 \
             --jars /home/ubuntu/postgresql-42.2.2.jar \
             --py-files $AUX_FILES \
             --executor-memory 4G \
             populate_database.py \
             $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE \
             2> /dev/null

echo "Spark Job completed"

unset EC2_MASTER_DNS
unset PGPASSWORD

cd ..

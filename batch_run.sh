#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/fieldsfromrawdata_small.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

EC2_MASTER_DNS=`grep ec2 $SPARK_HOME/conf/spark-env.sh | sed s/".*DNS.."//g | sed s/".$"//g`
PGPASSWORD=`cat ~/.pgpass | sed s/"\(.*:\)\{4\}"//g`

export EC2_MASTER_DNS
export PGPASSWORD

echo "Spark Job executing..."
#$SPARK_HOME/sbin/start-slaves.sh

cd batch_processing/

spark-submit --master spark://$EC2_MASTER_DNS:7077 \
             --jars /home/ubuntu/postgresql-42.2.2.jar \
             --py-files helpers.py \
             populate_database.py \
             $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE \
             2> /dev/null

echo "Spark Job completed"
#$SPARK_HOME/sbin/stop-slaves.sh

unset EC2_MASTER_DNS
unset PGPASSWORD

cd ..

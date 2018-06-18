#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/fieldsfromrawdata_small.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

MASTER=ec2-34-237-30-114.compute-1.amazonaws.com

SPARK_WORKER_INSTANCES=3
SPARK_WORKER_CORES=2
$SPARK_HOME/sbin/start-slaves.sh

cd batch_processing/

echo "Spark Job executing..."
spark-submit --master spark://$MASTER:7077 --jars /home/ubuntu/postgresql-42.2.2.jar --py-files helpers.py populate_database.py $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE 2> /dev/null
echo "Spark Job completed"

$SPARK_HOME/sbin/stop-slaves.sh
cd ..

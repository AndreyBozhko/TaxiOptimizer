#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/fieldsfromrawdata_small.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

MASTER=ec2-34-237-30-114.compute-1.amazonaws.com

cd batch_processing/

echo "Spark Job executing..."
spark-submit --master spark://$MASTER:7077 --jars /home/ubuntu/postgresql-42.2.2.jar --py-files helpers.py populate_database.py $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE 2> /dev/null
echo "Spark Job completed"

cd ..

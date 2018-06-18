#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/fieldsfromrawdata_small.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

cd batch_processing/

echo "Spark Job executing..."
spark-submit --jars /home/ubuntu/postgresql-42.2.2.jar populate_database.py $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE 2> /dev/null
echo "Spark Job completed"

cd ..

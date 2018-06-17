#!/bin/bash

TABLE=tbl
SCHEMAFILE=schema_postgres
PASSFILE=/home/ubuntu/.pgpass

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/rawfieldstouse.ini
PSQLCONFIGFILE=$PWD/config/postgresql.ini

cd batch_processing/

echo "Spark Job executing..."
spark-submit --jars /home/ubuntu/postgresql-42.2.2.jar populate_postgresql.py $S3CONFIGFILE $SCHEMAFILE $PSQLCONFIGFILE 2> /dev/null
echo "Spark Job completed"

cd ..

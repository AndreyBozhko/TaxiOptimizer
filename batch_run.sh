#!/bin/bash

. $PWD/load_configs.sh

spark-submit --master spark://$EC2_MASTER_DNS:7077 \
             --jars $PWD/postgresql-42.2.2.jar \
             --py-files $AUX_FILES \
             --executor-memory 4G \
             populate_database.py \
             $S3CONFIGFILE $SCHEMAFILE1 $PSQLCONFIGFILE \
             2> /dev/null

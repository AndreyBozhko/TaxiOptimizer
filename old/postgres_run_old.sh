#!/bin/bash

TABLE=tbl
SCHEMAFILE=schema_postgres
PASSFILE=/home/ubuntu/.pgpass

cd spark/

echo "Spark Job executing..."
spark-submit spark-submit --jars /home/ubuntu/postgresql-42.2.2.jar populate_table_postgres.py $TABLE $SCHEMAFILE $PASSFILE 2> /dev/null
echo "Spark Job completed"

cd ..

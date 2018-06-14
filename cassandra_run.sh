#!/bin/bash

KEYSPACE=ksp
TABLE=tbl

cd spark/

./cassandra_init.sh $KEYSPACE $TABLE schema

echo "Spark Job executing..."
spark-submit --packages anguenot:pyspark-cassandra:0.9.0 populate_table.py $KEYSPACE $TABLE 2> /dev/null
echo "Spark Job completed"

cd ..

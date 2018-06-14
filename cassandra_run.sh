#!/bin/bash

KEYSPACE=ksp
TABLE=tbl

cd spark/

./cassandra_init.sh $KEYSPACE $TABLE schema

spark-submit --packages anguenot:pyspark-cassandra:0.9.0 populate_table.py $KEYSPACE $TABLE

cd ..

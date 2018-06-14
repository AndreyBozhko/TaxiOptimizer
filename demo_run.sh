#!/bin/bash

KEYSPACE=ksp
TABLE=tbl

cd spark/

spark-submit --packages anguenot:pyspark-cassandra:0.9.0 run_demo.py $KEYSPACE $TABLE

cd ..

#!/bin/bash

KEYSPACE=$1
TABLE=$2
SCHEMAFILE=$3


cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"
cqlsh -e "CREATE KEYSPACE $KEYSPACE WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};"
echo "Created new Cassandra keyspace: $KEYSPACE"

cqlsh -e "CREATE TABLE $KEYSPACE.$TABLE `cat $SCHEMAFILE`;"
echo "Created new Cassandra table: $KEYSPACE.$TABLE"

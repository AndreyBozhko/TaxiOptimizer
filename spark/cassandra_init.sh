#!/bin/bash

KEYSPACE=$1
TABLE=$2
SCHEMAFILE=$3


cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"
cqlsh -e "CREATE KEYSPACE $KEYSPACE WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};"

cqlsh -e "CREATE TABLE $KEYSPACE.$TABLE `cat $SCHEMAFILE`;"

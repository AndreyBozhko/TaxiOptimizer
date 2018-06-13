#!/bin/bash

KEYSPACE=$1

cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"
cqlsh -e "CREATE KEYSPACE $KEYSPACE WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};"


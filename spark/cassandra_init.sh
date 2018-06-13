#!/bin/bash

KEYSPACE=$1
TABLE=$2
SCHEMA=$3


./create_keyspace.sh $KEYSPACE
./create_table.sh $KEYSPACE $TABLE $SCHEMA

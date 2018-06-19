#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/schema_for_streaming.ini
KAFKACONFIGFILE=$PWD/config/kafka.ini

python producer.py $KAFKACONFIGFILE $SCHEMAFILE $S3CONFIGFILE

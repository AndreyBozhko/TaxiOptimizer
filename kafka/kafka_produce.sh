#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
KAFKACONFIGFILE=$PWD/config/kafka.ini

python producer.py $KAFKACONFIGFILE $S3CONFIGFILE

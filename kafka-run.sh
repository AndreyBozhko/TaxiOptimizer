#!/bin/bash

S3CONFIGFILE=$PWD/config/s3bucket.ini
SCHEMAFILE=$PWD/config/schema_for_streaming.ini
KAFKACONFIGFILE=$PWD/config/kafka.ini

TOPIC=`grep TOPIC $KAFKACONFIGFILE | sed s/"TOPIC"//g | sed s/[:," "\"]//g`
NUM_PARTITIONS=`grep PARTITIONS $KAFKACONFIGFILE | sed s/[^[:digit:]]//g`
REPL_FACTOR=`grep REPL_FACTOR $KAFKACONFIGFILE | sed s/[^[:digit:]]//g`
ZOOKEEPER_IP=`grep ZOOKEEPER $KAFKACONFIGFILE | sed s/".*_IP"//g | sed s/[" "\"]//g | sed s/^.//g | sed s/.$//g`
BROKERS_IP=`grep BROKERS $KAFKACONFIGFILE | sed s/".*_IP"//g | sed s/[" "\"]//g | sed s/^.//g | sed s/.$//g`


convert () {

        ans=""    
        for item in `echo $1 | sed s/","/" "/g` ; do

                left=`echo $item | sed s/":.*"//g`
                right=`echo $item | sed s/".*:"//g`
                it=`echo $left | sed s/^.//g`
                ans="$ans,${!it}:$right"

        done

        ans=`echo $ans | sed s/^.//g`
        echo $ans
}

ZOOKEEPER_IP=$(convert $ZOOKEEPER_IP)
BROKERS_IP=$(convert $BROKERS_IP)


case $1 in

  --create)

    kafka-topics.sh --create --zookeeper $ZOOKEEPER_IP --topic $TOPIC --partitions $NUM_PARTITIONS --replication-factor $REPL_FACTOR
    ;;

  --produce)

    python kafka/producer.py $KAFKACONFIGFILE $SCHEMAFILE $S3CONFIGFILE &
    ;;

  --describe)

    kafka-topics.sh --describe --zookeeper $ZOOKEEPER_IP --topic $TOPIC
    ;;

  --delete)

    kafka-topics.sh --delete --zookeeper $ZOOKEEPER_IP --topic $TOPIC
    ;;

  --console-consume)

    kafka-console-consumer.sh --bootstrap-server $BROKERS_IP --from-beginning --topic $TOPIC
    ;;

  *)

    echo "Usage: ./kafka_run.sh [--create|--delete|--describe|--produce|--console-consume]"
    ;;

esac

#!/bin/bash

FOLDER=$AIRFLOW_HOME/dags
FILE=batch_scheduler.py

if [ ! -d $FOLDER ] ; then

    mkdir $FOLDER

fi

cp $FILE $FOLDER/
python $FOLDER/$FILE

#nohup airflow scheduler 2> /dev/null &
#nohup airflow webserver -p 8081 2> /dev/null &

#airflow trigger_dag batch_scheduler
airflow unpause batch_scheduler

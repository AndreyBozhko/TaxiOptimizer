#!/bin/bash

FOLDER=$AIRFLOW_HOME/dags
FILE=batch_scheduler.py

if [ ! -d $FOLDER ] ; then

    mkdir $FOLDER

fi

cp $FILE $FOLDER/
python $FOLDER/$FILE

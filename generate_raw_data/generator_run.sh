#!/bin/bash

EC2_MASTER_DNS=`grep ec2 $SPARK_HOME/conf/spark-env.sh | sed s/".*DNS.."//g | sed s/".$"//g`

BUCKET=andrey-bozhko-bucket-insight
FOLDER=nyc_taxi_raw_data/yellow

for year in 2009 2010 2011 2012 2013 2014 2015 ; do
  for month in 01 02 03 04 05 06 07 08 09 10 11 12 ; do

    spark-submit --master spark://$EC2_MASTER_DNS:7077 \
                 --executor-memory 4G \
                 generate.py \
                 $year $month $BUCKET $FOLDER

    for j in 1 2 ; do

        yy=`expr $year - $j '*' 7`

        FILENAME=trip_data_${yy}_${month}_s.csv
        PARTNAME=`aws s3 ls $BUCKET/$FOLDER/$FILENAME/ | grep part | sed s/".* part"/"part"/g`

        aws s3 mv s3://$BUCKET/$FOLDER/$FILENAME/$PARTNAME s3://$BUCKET/$FOLDER/$FILENAME
        aws s3 rm --recursive s3://$BUCKET/$FOLDER/$FILENAME/

    done

  done
done

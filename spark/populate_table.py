# needs pip install cassandra-driver

import sys
from math import floor
from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext
from datetime import datetime
from uuid import uuid4
#from cassandra.cluster import Cluster


def enforce_schema(msg):

    fields = msg.split(',')

    try:
	#dt = datetime.strptime(fields[5], '%d-%m-%y %H:%M')
	dt = datetime.strptime(fields[5], '%Y-%m-%d %H:%M:%S')
    except:
	return

    res = {"longitude": float(fields[10]), "latitude": float(fields[11])}
    if abs(res["longitude"]+74) > 0.24 or abs(res["latitude"]-40.75) > 0.24:
	return

    res["block_id"] = floor((res["longitude"]+74.25)*200)*100 + floor((res["latitude"]-40.5)*200)
    res["time_slot"] = (dt.hour*60+dt.minute)/10
    res["unique_id"] = str(uuid4())
    return res



if __name__ == '__main__':

    if len(sys.argv) != 3:
        sys.exit(-1)

    keyspace = sys.argv[1]
    table = sys.argv[2]

    conf = SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext(conf=conf)


    with open('s3config') as fin:
        config = {row[0]:row[1] for row in map(lambda s: s.strip().split('='), fin.readlines())}
        bucketname, foldername = config['BUCKET'], config['FOLDER']


    data = sc.textFile("s3a://{}/{}/*_small.csv".format(bucketname, foldername))
    data.map(lambda x: enforce_schema(x)).filter(lambda x: x is not None).saveToCassandra(keyspace, table)

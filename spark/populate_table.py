# needs pip install cassandra-driver

import sys
from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext
from helpers import determine_block_id, determine_time_slot, determine_top_spots
from uuid import uuid4


def enforce_schema(msg):

    fields = msg.split(',')
    res = {}

    res["longitude"], res["latitude"] = map(float, fields[10:12])
    res["time_slot"] = determine_time_slot(fields[5])
    res["block_id"] = determine_block_id(res["longitude"], res["latitude"])
    
    if res["block_id"] < 0 or res["time_slot"] < 0:	
	return

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


    data = sc.textFile("s3a://{}/{}/*.csv".format(bucketname, foldername))
    data.map(lambda x: enforce_schema(x)).filter(lambda x: x is not None).groupBy(lambda x: (x["block_id"], x["time_slot"])).mapValues(determine_top_spots).map(lambda x: (x[0][0], x[0][1], x[1])).saveToCassandra(keyspace, table)

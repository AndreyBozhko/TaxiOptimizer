# needs pip install cassandra-driver

import sys
from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext
from helpers import determine_block_ids, determine_time_slot
#from uuid import uuid4


def enforce_schema(msg):

    fields = msg.split(',')
    res = {}

    lon, lat = map(float, fields[10:12])
    res["passengers"] = int(fields[7])
    res["time_slot"] = determine_time_slot(fields[5])
    res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)


    if res["block_id"] < 0 or res["time_slot"] < 0:
        return

    #res["unique_id"] = str(uuid4())
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

    (data.map(lambda x: enforce_schema(x))
         .filter(lambda x: x is not None)
         .map(lambda x: ((x["block_id"], x["time_slot"], x["sub_block_id"]), x["passengers"]))
         .reduceByKey(lambda x,y : x+y)
         .map(lambda x: ((x[0][0], x[0][1]), [x[0][2], x[1]]))
         .groupByKey()
         .map(lambda x: {"block_id": x[0][0], "time_slot": x[0][1], "subblock_psgcnt": sorted(x[1], key=lambda z: -z[1])[:10]})
         .saveToCassandra(keyspace, table))

# needs pip install cassandra-driver

import time
import sys
import boto3
from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext
from helpers import determine_block_ids, determine_time_slot
#from uuid import uuid4


def enforce_schema(msg):

    fields = msg.split('\t')
    res = {}

    lat, lon = map(float, fields[0:2])
    res["time_slot"] = determine_time_slot(fields[2])
    res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)
    res["id"] = fields[3]
    
    
    if res["block_id"] < 0 or res["time_slot"] < 0:	
	return

    return res


def get_neighboring_blocks(block):
    return map(lambda x: x+block, [-1, 1, -100, 100, -99, 99, -101, 101])



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


    data = sc.cassandraTable(keyspace, table)
    
    while True:
	for line in boto3.client('s3').get_object(Bucket=bucketname, Key="nyc_taxi_raw_data/MTA-Bus-Time-2014-08-01-small.txt")["Body"]._raw_stream:
	    res = enforce_schema(line)
	    if res is None:
		continue

	    blocks = [[res["block_id"]]]
	    blocks.append(get_neighboring_blocks(res["block_id"]))

	    topspots = []
	    for i, blks in enumerate(blocks):
		for blk in blks:
		    topspots += data.select("block_id", "time_slot", "subblock_psgcnt").where("block_id={}".format(blk)).where("time_slot={}".format(res["time_slot"]+i)).collect()
	    print res
	    for tp in topspots:
		print tp
	    print
	    time.sleep(2)

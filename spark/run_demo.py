# needs pip install cassandra-driver

import time
import sys
import boto3
import random
from pyspark.context import SparkConf
from pyspark_cassandra import CassandraSparkContext
from helpers import determine_block_ids, determine_time_slot
#from uuid import uuid4


def enforce_schema(msg):

    fields = msg.split('\t')
    res = {}

    lat, lon = map(float, fields[0:2])
    res["date"] = fields[2]
    res["time_slot"] = determine_time_slot(fields[2])
    res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)
    res["id"] = fields[3]
    res["id"] = '0'*(4-len(res["id"]))+res["id"]
    res["latitude"], res["longitude"] = lat, lon
    
    
    if res["block_id"] < 0 or res["time_slot"] < 0:	
	return

    return res


def get_neighboring_blocks(block):
    return map(lambda x: x+block, [-1, 1, -100, 100, -99, 99, -101, 101])


def print_taxi(taxi):
    print ("Taxi {} || datetime={}  position=({:0.5f}, {:0.5f}) ||  timeslot={:3d}  block={:4d}  subblock={:3d}"
	  .format(taxi["id"], taxi["date"], taxi["latitude"], taxi["longitude"], taxi["time_slot"], taxi["block_id"], taxi["sub_block_id"]))

def print_spot(i, spot):
    print ("{}  ||  timeslot={:3d}  block={:4d}  subblock={:3d}  passengers={:2d}"
	  .format(i, spot[1], spot[0], spot[2], spot[3]))



def weighted_choice(weights):
    totals = []
    running_total = 0

    for w in weights:
        running_total += w
        totals.append(running_total)

    rnd = random.random() * running_total
    for i, total in enumerate(totals):
        if rnd < total:
            return i



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
	    taxi = enforce_schema(line)
	    if taxi is None:
		continue

	    blocks = [[taxi["block_id"]]]
	    blocks.append(get_neighboring_blocks(taxi["block_id"]))

	    topspots = []
	    for i, blks in enumerate(blocks):
		for blk in blks:
		    record = map(lambda x: [x["block_id"], x["time_slot"], x["subblock_psgcnt"]],
				(data.select("block_id", "time_slot", "subblock_psgcnt")
                                     .where("block_id={}".format(blk))
                                     .where("time_slot={}".format(taxi["time_slot"]+i))
                                     .collect()))

		    try:
			record = record[0]

	    	        for j, subblk in enumerate(record[2]):
			    topspots.append([record[0], record[1], subblk[0], subblk[1]])
		    except:
			pass

            topspots = sorted(topspots, key=lambda x:-x[3])[:10]

	    print_taxi(taxi)
	    if topspots != []:
		for i, tp in enumerate(topspots):
		    print_spot(i, tp)
	    
	        print "go to spot {}".format(weighted_choice(map(lambda x: x[3], topspots)))
	    else:
		print "not enough data"
	    
	    print "\n\n"
	    time.sleep(2)

# needs pip install cassandra-driver

import sys
import psycopg2
from helpers import *
from pyspark import *
from pyspark.sql import SQLContext


def enforce_schema(msg):
    fields = msg.split(',')
    res = {}

    lon, lat = map(float, fields[10:12])
    res["passengers"] = int(fields[7])
    res["time_slot"] = determine_time_slot(fields[5])
    res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)


    if res["block_id"] < 0 or res["time_slot"] < 0:
        return

    return res


def enforce_schema_by_header(msg, headerdict):
    fields = msg.split(',')
    res = {}

    lon, lat, psg, dt = map(lambda name: fields[headerdict[name]],
                            ["pickup_longitude", "pickup_latitude", "passenger_count", "pickup_datetime"])

    try:
        lon, lat = map(float, [lon, lat])
        res["passengers"] = int(psg)
        res["time_slot"] = determine_time_slot(dt)
        res["block_id"], res["sub_block_id"] = determine_block_ids(lon, lat)
    except:
        return

    if res["block_id"] < 0 or res["time_slot"] < 0:
        return

    return res


def infer_headerdict(headerstr, separator):
    return {s:i for i, s in enumerate(headerstr.split(separator))}


def get_s3_bucket_and_folder(configfile):
    with open(configfile) as fin:
        config = {row[0]:row[1] for row in map(lambda s: s.strip().split('='), fin.readlines())}
    return config['BUCKET'], config['FOLDER']



if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.exit(-1)

    table, schemafile, passwordfile = sys.argv[1:4]
    with open(passwordfile) as fin:
        password = fin.readline().strip().split(":")[-1]
        
    with open(schemafile) as fin:
        schema = fin.readline()

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    conn = psycopg2.connect("user=postgres host=localhost")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS {};".format(table))
    #cur.execute("CREATE TABLE {} {};".format(table, schema))
    conn.commit()

    bucketname, foldername = get_s3_bucket_and_folder('s3config')
    data = sc.textFile("s3a://{}/{}/*.csv".format(bucketname, foldername))

    data = (data.map(enforce_schema)
         .filter(lambda x: x is not None)
         .map(lambda x: ((x["block_id"], x["time_slot"], x["sub_block_id"]), x["passengers"]))
         .reduceByKey(lambda x,y : x+y)
         .map(lambda x: ((x[0][0], x[0][1]), [x[0][2], x[1]]))
         .groupByKey()
         .map(lambda x: {"block_id": x[0][0], "time_slot": x[0][1], "subblock_psgcnt": 0})) #sorted(x[1], key=lambda z: -z[1])[:10]}))

    sql_data = sqlContext.createDataFrame(data)
    url = 'jdbc:postgresql://localhost:5432/'
    properties = {'user': 'postgres', 'driver': 'org.postgresql.Driver'}

    (sql_data.write#.mode("append")
        .format("jdbc")
        .option("url", url)
        .option("user", properties["user"])
        .option("password", password)
        .option("driver", properties["driver"])
        .option("dbtable", table)
        .save())

    cur.close()
    conn.close()

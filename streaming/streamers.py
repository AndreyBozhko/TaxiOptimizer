import sys
sys.path.append("./helpers/")

import json
import time
import pyspark
import helpers
import numpy as np
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


####################################################################

class SparkStreamerFromKafka:
    """
    class that streams messages from Kafka topic and cleans up the message content
    """

    def __init__(self, kafka_configfile, schema_configfile, stream_configfile, start_offset):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type batch_interval:    float      time window for a single batch (in seconds)
        :type start_offset:      int        offset from which to read from partitions of Kafka topic
        """
        self.kafka_config  = helpers.parse_config(kafka_configfile)
        self.stream_config = helpers.parse_config(stream_configfile)
        self.schema        = helpers.parse_config(schema_configfile)

        self.start_offset = start_offset

        self.sc  = pyspark.SparkContext().getOrCreate()
        self.ssc = pyspark.streaming.StreamingContext(self.sc, self.stream_config["INTERVAL"])

        self.sc.setLogLevel("ERROR")


    def initialize_stream(self):
        """
        initializes stream from Kafka topic
        :type start_offset: int     offset from which to read from partitions of Kafka topic
        """
        topic, n = self.kafka_config["TOPIC"], self.kafka_config["PARTITIONS"]
        try:
            fromOffsets = {TopicAndPartition(topic, i): long(self.start_offset) for i in range(n)}
        except:
            fromOffsets = None

        self.dataStream = KafkaUtils.createDirectStream(self.ssc, [topic],
                                                {"metadata.broker.list": self.kafka_config["BROKERS_IP"]},
                                                fromOffsets=fromOffsets)


    def process_stream(self):
        """
        cleans the streamed data
        """
        self.initialize_stream()
        partitions = self.stream_config["PARTITIONS"]
        self.dataStream = (self.dataStream
                                    .repartition(partitions)
                                    .map(lambda x: json.loads(x[1]))
                                    .map(helpers.add_block_fields)
                                    .map(helpers.add_time_slot_field)
                                    .filter(lambda x: x is not None)
                                    .map(lambda x: ((x["time_slot"], x["block_id_x"], x["block_id_y"]),
                                                    (x["vehicle_id"], x["longitude"], x["latitude"], x["datetime"]))))


    def run(self):
        """
        starts streaming
        """
        self.process_stream()
        self.ssc.start()
        self.ssc.awaitTermination()


####################################################################

class TaxiStreamer(SparkStreamerFromKafka):
    """
    class that provides each taxi driver with the top-n pickup spots
    """

    def __init__(self, kafka_configfile, schema_configfile, stream_configfile, psql_configfile, start_offset=0):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), PostgreSQL database, data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type psql_configfile:   str        path to psql config file
        :type batch_interval:    float      time window for a single batch (in seconds)
        :type start_offset:      int        offset from which to read from partitions of Kafka topic
        """
        SparkStreamerFromKafka.__init__(self, kafka_configfile, schema_configfile, stream_configfile, start_offset)
        self.psql_config = helpers.get_psql_config(psql_configfile)
        self.load_batch_data()


    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database, splits it into BATCH_PARTS parts
        by time_slot field value and caches them
        """
        parts, partitions = self.stream_config["BATCH_PARTS"], self.stream_config["PARTITIONS"]

        sqlContext = pyspark.sql.SQLContext(self.sc)
        historic_data = (sqlContext.read.jdbc(url       =self.psql_config["url"],
                                              table     =self.psql_config["dbtable"],
                                              properties=self.psql_config["properties"])
                               .rdd.repartition(partitions)
                               .map(lambda x: x.asDict())
                               .map(lambda x: ((x["time_slot"], x["block_idx"], x["block_idy"]),
                                               (x["longitude"], x["latitude"], x["passengers"]))))

        historic_data.persist(pyspark.StorageLevel.MEMORY_ONLY_2)
        self.total = historic_data.map(lambda x: x[0][0]).distinct().count()

        total = self.total
        self.hdata = {}
        for tsl in range(parts):
            self.hdata[tsl] = historic_data.filter(lambda x: x[0][0]*parts/total==tsl)
            self.hdata[tsl].persist(pyspark.StorageLevel.MEMORY_ONLY_2)
            print "loaded batch {}/{} with {} rows".format(tsl+1, parts, self.hdata[tsl].count())

        historic_data.unpersist()


    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type time: datetime     timestamp for each RDD batch
        :type rdd:  RDD          Spark RDD from the stream
        """

        def my_join(x):
            """
            joins the record from table with historical data with the records of the taxi drivers' locations
            on the key (time_slot, block_id_x, block_id_y)
            schema for x: ((time_slot, block_idx, block_idy), (longitude, latitude, passengers))
            """
            try:
                return map(lambda el: (  el[0][0],
                                        (el[0][1], el[0][2]),
                                     zip( x[1][0],  x[1][1]),
                                        (el[1],     x[1][2])  ), rdd_bcast.value[x[0]])
            except:
                return [None]

        def select_customized_spots(x):
            """
            chooses no more than 3 pickup spots from top-n,
            based on the total number of rides from that spot
            and on the order in which the drivers send their location data
            schema for x: (vehicle_id, (longitude, latitude), [list of spots (lon, lat)], (i, [list of passenger pickups]))
            """
            try:
                length, total = len(x[3][1]), sum(x[3][1])
                np.random.seed(4040 + x[3][0])
                choices = np.random.choice(length, min(3, length), p=np.array(x[3][1])/float(total), replace=False)
                return (x[0], x[1], [x[2][c] for c in choices])
            except:
                return (x[0], x[1], [])


        global iPass
        try:
            iPass += 1
        except:
            iPass = 1

        print("========= RDD Batch Number: {0} - {1} =========".format(iPass, str(time)))

        try:
            parts, total = self.stream_config["BATCH_PARTS"], self.total

            # calculate list of distinct time_slots in current RDD batch
            tsl_list = rdd.map(lambda x: x[0][0]*parts/total).distinct().collect()

            # transform rdd and broadcast to workers
            # rdd_bcast has the following schema
            # rdd_bcast = {key: [list of value]}
            # key = (time_slot, block_id_x, block_id_y)
            # value = ((vehicle_id, longitude, latitude, datetime), i)
            # i shows the order in which drivers were sending data from certain block for certain time_slot
            rdd_bcast = (rdd.groupByKey()
                            .mapValues(lambda x: sorted(x, key=lambda el: el[3]))
                            .mapValues(lambda x: zip(x, xrange(len(x))))
                            .collect())
            if len(rdd_bcast) == 0:
                return

            rdd_bcast = self.sc.broadcast({x[0]:x[1] for x in rdd_bcast})

            # join the batch dataset with rdd_bcast, filter None values,
            # and from all the spot suggestions select specific for the driver to ensure no competition
            resDF = self.sc.union([(self.hdata[tsl]
                                          .flatMap(my_join, preservesPartitioning=True)
                                          .filter(lambda x: x is not None)
                                          .map(select_customized_spots)) for tsl in tsl_list])
            # output data
            # not actually implemented yet
            print resDF.count()

        except:
            pass


    def process_stream(self):
        """
        processes each RDD in the stream
        """
        SparkStreamerFromKafka.process_stream(self)

        process = self.process_each_rdd
        self.dataStream.foreachRDD(process)

import sys
sys.path.append("./helpers/")

import json
import time
import pyspark
import helpers
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
                                                    (x["vehicle_id"], x["longitude"], x["latitude"]))))


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
            print "loaded batch {} with {} rows".format(tsl+1, self.hdata[tsl].count())

        historic_data.unpersist()


    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type time: datetime     timestamp for each RDD batch
        :type rdd:  RDD          Spark RDD from the stream
        """

        def func(x):
            """
            joins the record from table with historical data with the records of the taxi drivers' locations
            on the key (time_slot, block_id_x, block_id_y)
            """
            try:
                return map(lambda el: (el, x[1]), rdd_bcast.value[x[0]])
            except:
                return [None]


        global iPass
        try:
            iPass += 1
        except:
            iPass = 1

        print("========= Microbatch Number: {0} - {1} =========".format(iPass, str(time)))

        try:
            parts, total = self.stream_config["BATCH_PARTS"], self.total
            tsl_list = rdd.map(lambda x: x[0][0]*parts/total).distinct().collect()

            rdd_bcast = rdd.groupByKey().collect()
            if len(rdd_bcast) == 0:
                return

            rdd_bcast = self.sc.broadcast({x[0]:x[1] for x in rdd_bcast})

            resDF = self.sc.union([(self.hdata[tsl]
                                          .flatMap(func, preservesPartitioning=True)
                                          .filter(lambda x: x is not None)) for tsl in tsl_list])
            print resDF.count()

            # resDF = self.sc.union([resDF, rdd.map(lambda x: ((x["vehicle_id"], x["longitude"], x["latitude"]), []))]).reduceByKey(lambda x,y: x+y)
            # print resDF.count()

        except:
            pass


    def process_stream(self):
        """
        processes each RDD in the stream
        """
        def updateFunc(new_values, current_sum):
            if current_sum is not None:
                last_sum = current_sum
            else:
                last_sum = 0
            return sum(map(lambda x: x, new_values)) + last_sum

        SparkStreamerFromKafka.process_stream(self)

        # Streaming analysis not implemented yet
        #
        # checkpoint = self.stream_configfile["CHECKPOINT"]
        # self.ssc.checkpoint(checkpoint)
        #
        # self.dataStream = self.dataStream.map(lambda x: (x[0], 1)).updateStateByKey(updateFunc)
        # self.dataStream.pprint(10)

        process = self.process_each_rdd
        self.dataStream.foreachRDD(process)

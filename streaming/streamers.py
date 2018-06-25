import sys
sys.path.append("./helpers/")

import json
import pyspark
import helpers
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


####################################################################

class SparkStreamerFromKafka:
    """
    class that streams messages from Kafka topic and cleans up the message content
    """

    def __init__(self, kafka_configfile, schema_configfile, batch_interval, start_offset):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type batch_interval:    float      time window for a single batch (in seconds)
        :type start_offset:      int        offset from which to read from partitions of Kafka topic
        """
        self.sc  = pyspark.SparkContext().getOrCreate()
        self.scc = pyspark.streaming.StreamingContext(self.sc, batch_interval)

        self.sc.setLogLevel("ERROR")

        self.kafka_config = helpers.parse_config(kafka_configfile)
        self.schema       = helpers.parse_config(schema_configfile)

        self.initialize_stream(start_offset)


    def initialize_stream(self, start_offset):
        """
        initializes stream from Kafka topic
        :type start_offset: int     offset from which to read from partitions of Kafka topic
        """
        topic, n = self.kafka_config["TOPIC"], self.kafka_config["PARTITIONS"]
        try:
            fromOffsets = {TopicAndPartition(topic, i): long(start_offset) for i in range(n)}
        except:
            fromOffsets = None

        self.dataStream = KafkaUtils.createDirectStream(self.scc, [topic],
                                            {"metadata.broker.list": self.kafka_config["BROKERS_IP"]},
                                            fromOffsets=fromOffsets)


    def process_stream(self):
        """
        cleans the streamed data
        """
        self.dataStream = (self.dataStream
                                    .map(lambda x: json.loads(x[1]))
                                    .map(helpers.add_block_fields)
                                    .map(helpers.add_time_slot_field)
                                    .filter(lambda x: x is not None))


    def run(self):
        """
        starts streaming
        """
        self.process_stream()
        self.scc.start()
        self.scc.awaitTermination()


####################################################################

class TaxiStreamer(SparkStreamerFromKafka):
    """
    class that provides each taxi driver with the top-n pickup spots
    """

    def __init__(self, kafka_configfile, schema_configfile, psql_configfile, batch_interval=1, start_offset=None):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), PostgreSQL database, data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type psql_configfile:   str        path to psql config file
        :type batch_interval:    float      time window for a single batch (in seconds)
        :type start_offset:      int        offset from which to read from partitions of Kafka topic
        """
        SparkStreamerFromKafka.__init__(self, kafka_configfile, schema_configfile, batch_interval, start_offset)
        self.psql_config = helpers.get_psql_config(psql_configfile)
        self.load_batch_data()


    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database and caches it
        """
        sqlContext = pyspark.sql.SQLContext(self.sc)
        self.historic_data = (sqlContext.read.jdbc(url      =self.psql_config["url"],
                                    table     =self.psql_config["dbtable"],
                                    properties=self.psql_config["properties"])
                               .rdd
                               .map(lambda x: x.asDict())
                               .map(lambda x: ((x["time_slot"], x["block_idx"], x["block_idy"]),
                                               (x["longitude"], x["latitude"], x["passengers"]))))

        self.historic_data.persist(pyspark.StorageLevel.MEMORY_ONLY)


    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type time: datetime     timestamp for each RDD batch
        :type rdd:  RDD          Spark RDD from the stream
        """

        def func(x):
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
            rdd_bcast = (rdd.map(lambda x: ((x["time_slot"], x["block_id_x"], x["block_id_y"]),
                                            (x["vehicle_id"], x["longitude"], x["latitude"])))
                            .groupByKey().collect())
            if len(rdd_bcast) == 0:
                return

            rdd_bcast = self.sc.broadcast({x[0]:x[1] for x in rdd_bcast})

            resDF = self.historic_data.flatMap(func).filter(lambda x: x is not None)
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
